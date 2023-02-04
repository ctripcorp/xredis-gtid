/* Copyright (c) 2023, ctrip.com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <assert.h>
#include "util.h"
#include "gtid.h"

#ifndef GTID_MALLOC_INCLUDE
#define GTID_MALLOC_INCLUDE "gtid_malloc.h"
#endif

#include GTID_MALLOC_INCLUDE

#define GNO_REPR_MAX_LEN 21

#define MIN(a, b)	(a) < (b) ? a : b
#define MAX(a, b)	(a) < (b) ? b : a

#define MOVE /* ownership moved. */

#define GTID_INTERVAL_SKIPLIST_MAXLEVEL 32 /* Should be enough for 2^64 elements */
#define GTID_INTERVAL_SKIPLIST_P 0.25      /* Skiplist P = 1/4 */

static int gtidIntervalDecode(char* interval_str, size_t len, gno_t *pstart,
        gno_t *pend) {
    const char *hyphen = "-";
    int index = -1;
    for(int i = 0; i < len; i++) {
        if(interval_str[i] == hyphen[0]) {
            index = i;
            break;
        }
    }
    gno_t start = 0, end = 0;
    if(index == -1) {
        if(string2ll(interval_str, len, &start)) {
            if (pstart) *pstart = start;
            if (pend) *pend = start;
            return 0;
        }
    } else {
        /* {start}-{end} */
        if(string2ll(interval_str, index, &start) &&
         string2ll(interval_str+index+1, len-index-1, &end)) {
            if (pstart) *pstart = start;
            if (pend) *pend = end;
            return 0;
        }
    }
    return 1;
}

ssize_t gtidIntervalEncode(char *buf, size_t maxlen, gno_t start, gno_t end) {
    size_t len = 0;
    if (len+GNO_REPR_MAX_LEN > maxlen) goto err;
    len += ll2string(buf+len, GNO_REPR_MAX_LEN, start);
    if (start != end) {
        if (len+1+GNO_REPR_MAX_LEN > maxlen) goto err;
        memcpy(buf+len, "-", 1), len += 1;
        len += ll2string(buf + len, GNO_REPR_MAX_LEN, end);
    }
    return len;
err:
    return -1;
}

gtidIntervalNode *gtidIntervalNodeNew(int level, gno_t start, gno_t end) {
    assert(start <= end);
    size_t intvl_size = sizeof(gtidIntervalNode)+level*sizeof(gtidIntervalNode*);
    gtidIntervalNode *interval = gtid_malloc(intvl_size);
    memset(interval,0,intvl_size);
    interval->start = start;
    interval->end = end;
    return interval;
}

void gtidIntervalNodeFree(gtidIntervalNode* interval) {
    gtid_free(interval);
}

gtidIntervalSkipList *gtidIntervalSkipListNew() {
    gtidIntervalSkipList *gsl = gtid_malloc(sizeof(*gsl));
    gsl->level = 1;
    gsl->header = gtidIntervalNodeNew(GTID_INTERVAL_SKIPLIST_MAXLEVEL,0,0);
    gsl->tail = gsl->header;
    gsl->node_count = 1;
    gsl->gno_count = 0;
    return gsl;
}

void gtidIntervalSkipListFree(gtidIntervalSkipList *gsl) {
	gtidIntervalNode *interval = gsl->header->forwards[0], *next;

    gtid_free(gsl->header);
    while(interval) {
        next = interval->forwards[0];
        gtidIntervalNodeFree(interval);
        interval = next;
    }
    gtid_free(gsl);
}

static int gitdIntervalRandomLevel(void) {
    int level = 1;
    while ((rand()&0xFFFF) < (GTID_INTERVAL_SKIPLIST_P * 0xFFFF)) /* TODO random ? */
        level += 1;
    return (level<GTID_INTERVAL_SKIPLIST_MAXLEVEL) ? level : GTID_INTERVAL_SKIPLIST_MAXLEVEL;
}

static inline size_t gtidIntervalNodeGnoCount(struct gtidIntervalNode *interval) {
    return interval->end - interval->end + 1;
}

/* return num of gno added. */
gno_t gtidIntervalSkipListAdd(gtidIntervalSkipList *gsl, gno_t start, gno_t end) {
    gtidIntervalNode *lefts[GTID_INTERVAL_SKIPLIST_MAXLEVEL],
                 *rights[GTID_INTERVAL_SKIPLIST_MAXLEVEL], *x, *l, *r;
    int i, level;
    gno_t added = 0;

	assert(start <= end);

    x = gsl->header;
    for (i = gsl->level-1; i >= 0; i--) {
        while (x->forwards[i] && x->forwards[i]->end + 1 < start)
            x = x->forwards[i];
        lefts[i] = x;
    }

    x = gsl->header;
    for (i = gsl->level-1; i >= 0; i--) {
        while (x->forwards[i] && x->forwards[i]->start < end + 1)
            x = x->forwards[i];
        rights[i] = x;
    }

	if (lefts[0] == rights[0]) {
       /* none overlaps with [start, end]: create new one. */
	   level = gitdIntervalRandomLevel();
	   x = gtidIntervalNodeNew(level,start,end);
	   if (level > gsl->level) {
		   for (i = gsl->level; i < level; i++) {
			   lefts[i] = gsl->header;
			   rights[i] = x;
		   }
		   gsl->level = level;
	   }
	   for (i = gsl->level-1; i >= 0; i--) {
		   lefts[i]->forwards[i] = rights[i]->forwards[i];
	   }
       added = end-start+1;
       if (gsl->tail->forwards[0] != NULL)
           gsl->tail = gsl->tail->forwards[0];
       gsl->gno_count += added;
       gsl->node_count ++;
    } else {
        /* overlaps with [start, end]: join all into leftmost and remove others. */
        size_t saved_gno_count;
        gtidIntervalNode *saved_tail, *next;
        l = lefts[0]->forwards[0], r = rights[0]->forwards[0];
        saved_gno_count = gtidIntervalNodeGnoCount(l);
        l->start = MIN(start,l->start);
        l->end = MAX(end,rights[0]->end);
        gsl->gno_count += gtidIntervalNodeGnoCount(l) - saved_gno_count;
        /* tail will be removed, update tail to l */
        if (gsl->tail == r) gsl->tail = l;

        x = l->forwards[0];
        while (x != r) {
            next = x->forwards[0];
            gsl->gno_count -= gtidIntervalNodeGnoCount(l);
            gsl->node_count--;
            gtidIntervalNodeFree(l);
            x = next;
        }

        for (i = gsl->level-1; i >= 0; i--) {
            lefts[i]->forwards[i] = rights[i]->forwards[i];
        }

        while(gsl->level > 1 && gsl->header->forwards[gsl->level-1] == NULL)
            gsl->level--;
    }

	return added;
}

gno_t gtidIntervalSkipListMerge(gtidIntervalSkipList *dst,
        MOVE gtidIntervalSkipList *src) {
    gno_t added = 0;
    gtidIntervalNode *x;
    for (x = src->header; x != NULL; x = x->forwards[0]) {
        added += gtidIntervalSkipListAdd(dst, x->start, x->end);
    }
    gtidIntervalSkipListFree(src);
    return added;
}

int gtidIntervalSkipListContains(gtidIntervalSkipList *gsl, gno_t gno) {
    int i, level;
    gtidIntervalNode *x = gsl->header;
    assert(gno > 0);
    for (i = gsl->level-1; i >= 0; i--) {
        while (x->forwards[i] && x->forwards[i]->start <= gno)
            x = x->forwards[i];
    }
    return x->start <= gno && gno <= x->end;
}

gno_t gtidIntervalSkipListAdvance(gtidIntervalSkipList *gsl) {
    if (gsl->header == gsl->tail) {
        gtidIntervalSkipListAdd(gsl,GNO_INITIAL,GNO_INITIAL);
        return GNO_INITIAL;
    } else {
        gsl->tail->end++;
        return gsl->tail->end;
    }
}

/* return -1 if encode failed, otherwise return encoded length. */
ssize_t uuidGnoEncode(char *buf, size_t maxlen, const char *uuid,
        size_t uuid_len, gno_t gno) {
    size_t len = 0;
    if (maxlen < uuid_len+1+GNO_REPR_MAX_LEN) return -1;
    memcpy(buf + len, uuid, uuid_len), len += uuid_len;
    memcpy(buf + len, ":", 1), len += 1;
    len += ll2string(buf + len, GNO_REPR_MAX_LEN, gno);
    return len;
}

char* uuidGnoDecode(char* src, size_t src_len, long long* gno, int* uuid_len) {
    const char *split = ":";
    int index = -1;
    for(int i = 0; i < src_len; i++) {
        if(src[i] == split[0]) {
            index = i;
            break;
        }
    }
    if(index == -1) goto err;
    if(string2ll(src+index+1, src_len-index-1, gno) == 0) goto err;
    *uuid_len = index;
    return src;
err:
    return NULL;
}

static void uuidDup(char **pdup, size_t *pdup_len, const char* uuid,
        int uuid_len) {
	char* dup = gtid_malloc(uuid_len + 1);
	memcpy(dup, uuid, uuid_len);
	dup[uuid_len] = '\0';
    *pdup = dup;
    *pdup_len = uuid_len;
}

uuidSet *uuidSetNew(const char *uuid, size_t uuid_len) {
    uuidSet *uuid_set = gtid_malloc(sizeof(*uuid_set));
    uuidDup(&uuid_set->uuid,&uuid_set->uuid_len,uuid,uuid_len);
    uuid_set->intervals = gtidIntervalSkipListNew();
    uuid_set->next = NULL;
    return uuid_set;
}

void uuidSetFree(uuidSet* uuid_set) {
    gtidIntervalSkipListFree(uuid_set->intervals);
    gtid_free(uuid_set->uuid);
    gtid_free(uuid_set);
}

uuidSet *uuidSetDecode(char* uuid_set_str, int len) {
    const char *colon = ":";
    if(uuid_set_str[len - 1] == colon[0]) {
        return NULL;
    }
    uuidSet *uuid_set = NULL;
    int start_index = 0;
    for(int i = 0; i < len; i++) {
        if(uuid_set_str[i] == colon[0]) {
            assert(uuid_set == NULL);
            uuid_set = uuidSetNew(uuid_set_str, i);
            start_index = i + 1;
            break;
        }
    }
    if(uuid_set == NULL) {
        return NULL;
    }
    int end_index = len - 1;
    int count = 0;
    gno_t start, end;
    for(int i = len - 2; i >= start_index; i--) {
        if(uuid_set_str[i] == colon[0]) {
            if(i == end_index) goto err;
            gtidIntervalDecode(uuid_set_str+i+1, end_index-i, &start, &end);
            uuidSetAdd(uuid_set, start, end);
            end_index = i - 1;
            count++;
        }
    }
    gtidIntervalDecode(uuid_set_str+start_index, end_index-start_index+1,
            &start, &end);
    uuidSetAdd(uuid_set, start, end);
    return uuid_set;
err:
    if(uuid_set != NULL) {
        uuidSetFree(uuid_set);
        uuid_set = NULL;
    }
    return NULL;
}

/* {uuid}: {longlong}-{longlong}* n */
size_t uuidSetEstimatedEncodeBufferSize(uuidSet* uuid_set) {
    /* 44 = 1(:) + 21(longlong) + 1(-) + 21(long long) */
    return uuid_set->uuid_len + uuid_set->intervals->node_count * 44;
}

ssize_t uuidSetEncode(char *buf, size_t maxlen, uuidSet* uuid_set) {
    size_t len = 0, ret;

    if (len+uuid_set->uuid_len > maxlen) goto err;
    memcpy(buf + len, uuid_set->uuid, uuid_set->uuid_len);
    len += uuid_set->uuid_len;

    for (gtidIntervalNode *cur = uuid_set->intervals->header->forwards[0];
            cur != NULL; cur = cur->forwards[0]) {
        if (len+1 > maxlen) goto err;
        memcpy(buf + len, ":", 1), len += 1;
        ret = gtidIntervalEncode(buf+len, maxlen-len, cur->start, cur->end);
        if (ret < 0) goto err;
        len += ret;
    }
    return len;
err:
    return -1;
}

gno_t uuidSetAdd(uuidSet* uuid_set, gno_t start, gno_t end)  {
    return gtidIntervalSkipListAdd(uuid_set->intervals,start,end);
}

gno_t uuidSetRaise(uuidSet *uuid_set, gno_t watermark) {
    return gtidIntervalSkipListAdd(uuid_set->intervals,1,watermark);
}

gno_t uuidSetMerge(uuidSet* dst, MOVE uuidSet* src) {
    if(dst->uuid_len != src->uuid_len ||
            memcmp(dst->uuid, src->uuid, src->uuid_len) != 0) {
        return 0;
    }
    return gtidIntervalSkipListMerge(dst->intervals, src->intervals);
}

int uuidSetContains(uuidSet* uuid_set, gno_t gno) {
    return gtidIntervalSkipListContains(uuid_set->intervals, gno);
}

gno_t uuidSetAdvance(uuidSet* uuid_set) {
    return gtidIntervalSkipListAdvance(uuid_set->intervals);
}

gtidSet* gtidSetNew() {
    gtidSet *gtid_set = gtid_malloc(sizeof(*gtid_set));
    gtid_set->header = NULL;
    gtid_set->tail = NULL;
    return gtid_set;
}

void gtidSetFree(gtidSet *gtid_set) {
    uuidSet *cur = gtid_set->header, *next;
    while(cur != NULL) {
        next = cur->next;
        uuidSetFree(cur);
        cur = next;
    }
    gtid_free(gtid_set);
}

gno_t gtidSetAppend(gtidSet *gtid_set, MOVE uuidSet *uuid_set) {
    if (gtid_set->header == NULL) {
        gtid_set->header = uuid_set;
        gtid_set->tail = uuid_set;
    } else {
        gtid_set->tail->next = uuid_set;
        gtid_set->tail = uuid_set;
    }
    return uuid_set->intervals->gno_count;
}

gtidSet *gtidSetDecode(char* src, size_t len) {
    gtidSet* gtid_set = gtidSetNew();
    const char *split = ",";
    int uuid_str_start_index = 0;
    for(int i = 0; i < len; i++) {
        if(src[i] == split[0]) {
            uuidSet *uuid_set = uuidSetDecode(src+uuid_str_start_index,
                    i-uuid_str_start_index);
            gtidSetAppend(gtid_set, uuid_set);
            uuid_str_start_index = (i + 1);
        }
    }
    uuidSet *uuid_set = uuidSetDecode(src+uuid_str_start_index,
            len-uuid_str_start_index);
    gtidSetAppend(gtid_set, uuid_set);
    return gtid_set;
}

size_t gtidSetEstimatedEncodeBufferSize(gtidSet* gtid_set) {
    size_t max_len = 1; // must > 0;
    uuidSet *cur = gtid_set->header;
    while(cur != NULL) {
        max_len += uuidSetEstimatedEncodeBufferSize(cur) + 1;
        cur = cur->next;
    }
    return max_len;
}

ssize_t gtidSetEncode(char *buf, size_t maxlen, gtidSet* gtid_set) {
    size_t len = 0, ret;
    for (uuidSet *cur = gtid_set->header;cur != NULL; cur = cur->next) {
        ret = uuidSetEncode(buf+len, maxlen-len, cur);
        if (ret < 0) goto err;
        len += ret;
        if (len+1 > maxlen) goto err;
        memcpy(buf+len, ",", 1), len += 1;
    }
    return len;
err:
    return -1;
}

uuidSet* gtidSetFind(gtidSet* gtid_set, const char* uuid, size_t uuid_len) {
    uuidSet *cur = gtid_set->header;
    while(cur != NULL) {
        if (cur->uuid_len == uuid_len &&
                memcmp(cur->uuid, uuid, uuid_len) == 0) {
            break;
        }
        cur = cur->next;
    }
    return cur;
}

gno_t gtidSetAdd(gtidSet* gtid_set, const char* uuid, size_t uuid_len, gno_t gno) {
    uuidSet *cur = gtidSetFind(gtid_set, uuid, uuid_len);
    if (cur == NULL) {
        cur = uuidSetNew(uuid, uuid_len);
        gtidSetAppend(gtid_set, cur);
    }
    return uuidSetAdd(cur, gno, gno);
}

gno_t gtidSetRaise(gtidSet* gtid_set, const char* uuid, size_t uuid_len, gno_t watermark) {
    if (watermark < GNO_INITIAL) return 0;
    uuidSet *cur = gtidSetFind(gtid_set, uuid, uuid_len);
    if (cur == NULL) {
        cur = uuidSetNew(uuid, uuid_len);
        gtidSetAppend(gtid_set, cur);
    }
    return uuidSetAdd(cur, GNO_INITIAL, watermark);
}

gno_t gtidSetMerge(gtidSet* dst, MOVE gtidSet* src) {
    gno_t added = 0;
    uuidSet *cur = src->header;
    while(cur != NULL) {
        uuidSet* src = gtidSetFind(dst, cur->uuid, cur->uuid_len);
        if(src != NULL) {
            added += uuidSetMerge(src, cur);
        } else {
            added += gtidSetAppend(dst, cur);
        }
        cur = cur->next;
    }
    return added;
}

/* TODO remove
size_t uuidSetAdvanceEncode(uuidSet* uuid_set, char* buf) {
    size_t len = 0;
    gno_t gno = uuidSetAdvance(uuid_set);
    memcpy(buf + len, uuid_set->uuid, uuid_set->uuid_len);
    len += uuid_set->uuid_len;
    memcpy(buf + len, ":", 1), len += 1;
    len += ll2string(buf + len, 21, gno);
    return len;
} */

