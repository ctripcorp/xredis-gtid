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
#include "gtid_util.h"
#include "gtid.h"

#ifndef GTID_MALLOC_INCLUDE
#define GTID_MALLOC_INCLUDE "gtid_malloc.h"
#endif

#include GTID_MALLOC_INCLUDE

#define GNO_REPR_MAX_LEN 21

#define MIN(a, b)	(a) < (b) ? (a) : (b)
#define MAX(a, b)	(a) < (b) ? (b) : (a)

#define GTID_INTERVAL_SKIPLIST_MAXLEVEL 32 /* Should be enough for 2^64 elements */
#define GTID_INTERVAL_SKIPLIST_P 0.25      /* Skiplist P = 1/4 */

/* most gtid interval nodes are level 1 */
#define GTID_INTERVAL_MEMORY (sizeof(gtidIntervalNode) + sizeof(gtidIntervalNode*))

static inline int gtidIntervalIsValid(gno_t start, gno_t end) {
    return start >= GTID_GNO_INITIAL && start <= end;
}

int gtidIntervalDecode(char* interval_str, size_t len, gno_t *pstart,
        gno_t *pend) {
    const char *hyphen = "-";
    int index = -1;
    for(size_t i = 0; i < len; i++) {
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
    size_t intvl_size = sizeof(gtidIntervalNode)+level*sizeof(gtidIntervalNode*);
    gtidIntervalNode *interval = gtid_malloc(intvl_size);
    memset(interval,0,intvl_size);
    interval->level = level;
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
    gtidIntervalNode *interval = gsl->header, *next;
    while(interval) {
        next = interval->forwards[0];
        gtidIntervalNodeFree(interval);
        interval = next;
    }
    gtid_free(gsl);
}

gtidIntervalSkipList *gtidIntervalSkipListDup(gtidIntervalSkipList *gsl) {
    gtidIntervalNode *leads[GTID_INTERVAL_SKIPLIST_MAXLEVEL], *cur, *tail, *x;
    gtidIntervalSkipList *dup = gtid_malloc(sizeof(*gsl));

    dup->level = gsl->level;
    dup->node_count = gsl->node_count;
    dup->gno_count = gsl->gno_count;
    dup->header = gtidIntervalNodeNew(GTID_INTERVAL_SKIPLIST_MAXLEVEL,0,0);

    for (int i = 0; i < GTID_INTERVAL_SKIPLIST_MAXLEVEL; i++)
        leads[i] = dup->header;

    tail = dup->header;
    cur = gsl->header->forwards[0];
    while (cur) {
        x = gtidIntervalNodeNew(cur->level,cur->start,cur->end);
        for (int level = 0; level < x->level; level++) {
            leads[level]->forwards[level] = x;
            leads[level] = x;
        }
        tail = x;
        cur = cur->forwards[0];
    }

    dup->tail = tail;
    return dup;
}

static int gitdIntervalRandomLevel(void) {
    int level = 1;
    while ((rand()&0xFFFF) < (GTID_INTERVAL_SKIPLIST_P * 0xFFFF))
        level += 1;
    return (level<GTID_INTERVAL_SKIPLIST_MAXLEVEL) ? level : GTID_INTERVAL_SKIPLIST_MAXLEVEL;
}

static inline size_t gtidIntervalNodeGnoCount(struct gtidIntervalNode *interval) {
    return interval->end - interval->start + 1;
}

/* return num of gno added. */
gno_t gtidIntervalSkipListAdd(gtidIntervalSkipList *gsl, gno_t start, gno_t end) {
    gtidIntervalNode *lefts[GTID_INTERVAL_SKIPLIST_MAXLEVEL],
                 *rights[GTID_INTERVAL_SKIPLIST_MAXLEVEL], *x, *l, *r;
    int i, level;
    ssize_t added = 0;

    assert(gtidIntervalIsValid(start, end));

    /* fast path */
    if (gsl->header != gsl->tail && gsl->tail->end+1 == start) {
        gsl->tail->end = end;
        added = end+1-start;
        gsl->gno_count+=added;
        return added;
    }

    x = gsl->header;
    for (i = gsl->level-1; i >= 0; i--) {
        while (x->forwards[i] && x->forwards[i]->end + 1 < start)
            x = x->forwards[i];
        lefts[i] = x;
    }

    x = gsl->header;
    for (i = gsl->level-1; i >= 0; i--) {
        while (x->forwards[i] && end + 1 >= x->forwards[i]->start)
            x = x->forwards[i];
        rights[i] = x;
    }

    if (lefts[0] == rights[0]) {
        /* none overlaps with [start, end]: create new one. */
        level = gitdIntervalRandomLevel();
        x = gtidIntervalNodeNew(level,start,end);

       if (level > gsl->level) {
           for (i = gsl->level; i < level; i++)
               rights[i] = gsl->header;
           gsl->level = level;
       }

       for (i = 0; i < x->level; i++) {
           x->forwards[i] = rights[i]->forwards[i];
           rights[i]->forwards[i] = x;
       }

       added = end-start+1;
       gsl->gno_count += added;
       gsl->node_count++;

       if (gsl->tail->forwards[0]) gsl->tail = gsl->tail->forwards[0];
    } else {
        /* overlaps with [start, end]: join all to rightmost and remove others. */
        size_t saved_gno_count;
        gtidIntervalNode *next;

        l = lefts[0]->forwards[0], r = rights[0], x = rights[0];
        saved_gno_count = gtidIntervalNodeGnoCount(x);
        x->start = MIN(start,l->start);
        x->end = MAX(end,r->end);
        added += gtidIntervalNodeGnoCount(x) - saved_gno_count;

        for (i = 0; i < x->level; i++)
            lefts[i]->forwards[i] = x;
        for (i = x->level; i < gsl->level; i++)
            lefts[i]->forwards[i] = rights[i]->forwards[i];

        while (l && l != r) {
            next = l->forwards[0];
            gsl->node_count--;
            added -= gtidIntervalNodeGnoCount(l);
            gtidIntervalNodeFree(l);
            l = next;
        }

        while(gsl->level > 1 && gsl->header->forwards[gsl->level-1] == NULL)
            gsl->level--;
        gsl->gno_count += added;
    }

    return added;
}

gno_t gtidIntervalSkipListMerge(gtidIntervalSkipList *dst,
        gtidIntervalSkipList *src) {
    gno_t added = 0;
    gtidIntervalNode *x;
    for (x = src->header->forwards[0]; x != NULL; x = x->forwards[0]) {
        added += gtidIntervalSkipListAdd(dst, x->start, x->end);
    }
    return added;
}

gno_t gtidIntervalSkipListRemove(gtidIntervalSkipList *gsl, gno_t start,
        gno_t end) {
    gtidIntervalNode *lefts[GTID_INTERVAL_SKIPLIST_MAXLEVEL],
                 *rights[GTID_INTERVAL_SKIPLIST_MAXLEVEL], *x;
    int i;
    ssize_t removed = 0;

    assert(gtidIntervalIsValid(start, end));

    /* lefts: last (whole or partial) node to reserve */
    x = gsl->header;
    for (i = gsl->level-1; i >= 0; i--) {
        while (x->forwards[i] && x->forwards[i]->start < start)
            x = x->forwards[i];
        lefts[i] = x;
    }

    /* rights: last whole node to remove */
    x = gsl->header;
    for (i = gsl->level-1; i >= 0; i--) {
        while (x->forwards[i] && x->forwards[i]->end <= end)
            x = x->forwards[i];
        rights[i] = x;
    }

    if (rights[0]->end < lefts[0]->start) {
        /* remove gno within one node: split it. */
        int level = gitdIntervalRandomLevel();
        x = gtidIntervalNodeNew(level,end+1,lefts[0]->end);
        lefts[0]->end = start-1;

        if (level > gsl->level) {
            for (i = gsl->level; i < level; i++)
                lefts[i] = gsl->header;
            gsl->level = level;
        }

        for (i = 0; i < x->level; i++) {
            x->forwards[i] = lefts[i]->forwards[i];
            lefts[i]->forwards[i] = x;
        }

        removed = end-start+1;
        gsl->gno_count -= removed;
        gsl->node_count++;
        if (gsl->tail->forwards[0]) gsl->tail = gsl->tail->forwards[0];
    } else {
        size_t saved_gno_count;
        gtidIntervalNode *l, *r, *next;

        l = lefts[0];
        saved_gno_count = gtidIntervalNodeGnoCount(l);
        l->end = MIN(l->end,start-1);
        removed += saved_gno_count - gtidIntervalNodeGnoCount(l);

        r = rights[0]->forwards[0];
        if (r) {
            saved_gno_count = gtidIntervalNodeGnoCount(r);
            r->start = MAX(r->start,end+1);
            removed += saved_gno_count - gtidIntervalNodeGnoCount(r);
        } else {
            gsl->tail = l;
        }

        l = lefts[0]->forwards[0];

        for (i = 0; i < gsl->level; i++)
            lefts[i]->forwards[i] = rights[i]->forwards[i];

        while (l && l != r) {
            next = l->forwards[0];
            gsl->node_count--;
            removed += gtidIntervalNodeGnoCount(l);
            gtidIntervalNodeFree(l);
            l = next;
        }

        while(gsl->level > 1 && gsl->header->forwards[gsl->level-1] == NULL)
            gsl->level--;
        gsl->gno_count -= removed;
    }

    return removed;
}

gno_t gtidIntervalSkipListDiff(gtidIntervalSkipList *dst,
        gtidIntervalSkipList *src) {
    gno_t removed = 0;
    gtidIntervalNode *cur = src->header->forwards[0], *next;
    while (cur) {
        next = cur->forwards[0];
        removed += gtidIntervalSkipListRemove(dst, cur->start, cur->end);
        cur = next;
    }
    return removed;
}

int gtidIntervalSkipListContains(gtidIntervalSkipList *gsl, gno_t gno) {
    int i;
    gtidIntervalNode *x = gsl->header;
    assert(gno >= GTID_GNO_INITIAL);
    for (i = gsl->level-1; i >= 0; i--) {
        while (x->forwards[i] && x->forwards[i]->start <= gno)
            x = x->forwards[i];
    }
    return x->start <= gno && gno <= x->end;
}

gno_t gtidIntervalSkipListNext(gtidIntervalSkipList *gsl, int update) {
    gno_t gno = gsl->tail->end+1;
    if (update) gtidIntervalSkipListAdd(gsl, gno, gno);
    return gno;
}

gno_t gtidIntervalSkipListCurrent(gtidIntervalSkipList *gsl) {
    return gsl->tail->end;
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

char* uuidGnoDecode(char* src, size_t src_len, long long* gno, size_t* uuid_len) {
    const char *split = ":";
    int index = -1;
    for(size_t i = 0; i < src_len; i++) {
        if(src[i] == split[0]) {
            index = i;
            break;
        }
    }
    if(index == -1) goto err;
    if(string2ll(src+index+1, src_len-index-1, gno) == 0) goto err;
    *uuid_len = (size_t)index;
    return src;
err:
    return NULL;
}

void uuidDup(char **pdup, size_t *pdup_len, const char* uuid,
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

uuidSet *uuidSetDup(uuidSet* uuid_set) {
    uuidSet *result = gtid_malloc(sizeof(uuidSet));
    uuidDup(&result->uuid,&result->uuid_len,uuid_set->uuid,uuid_set->uuid_len);
    result->intervals = gtidIntervalSkipListDup(uuid_set->intervals);
    result->next = NULL;
    return result;
}

gno_t uuidSetCount(uuidSet *uuid_set) {
    return uuid_set->intervals->gno_count;
}

uuidSet *uuidSetDecode(char* uuid_set_str, int len) {
    const char *colon = ":";
    uuidSet *uuid_set = NULL;

    if(len <= 0) goto err;

    int i;
    for(i = 0; i < len; i++) {
        if(uuid_set_str[i] == colon[0]) break;
    }
    uuid_set = uuidSetNew(uuid_set_str, i);

    /* this is an empty uuidSet */
    if (i == len) return uuid_set;

    int ret, start_index = i+1, end_index = i+1;
    gno_t start, end;
    for (;end_index < len; end_index++) {
        if (uuid_set_str[end_index] == colon[0]) {
            if (start_index < end_index) {
                ret = gtidIntervalDecode(uuid_set_str+start_index,
                        end_index-start_index, &start, &end);
                if (ret == 0 && gtidIntervalIsValid(start,end)) {
                    uuidSetAdd(uuid_set, start, end);
                } else {
                    goto err;
                }
            }
            start_index = end_index+1;
        }
    }

    if (start_index < end_index) {
        ret = gtidIntervalDecode(uuid_set_str+start_index,
                end_index-start_index, &start, &end);
        if (ret == 0 && gtidIntervalIsValid(start,end)) {
            uuidSetAdd(uuid_set, start, end);
        } else {
            goto err;
        }
    }

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
    gtidIntervalNode *cur, *header;

    if (len+uuid_set->uuid_len > maxlen) goto err;
    memcpy(buf + len, uuid_set->uuid, uuid_set->uuid_len);
    len += uuid_set->uuid_len;

    header = uuid_set->intervals->header->forwards[0];
    for (cur = header; cur != NULL; cur = cur->forwards[0]) {
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
    if (!gtidIntervalIsValid(start, end)) return 0;
    return gtidIntervalSkipListAdd(uuid_set->intervals,start,end);
}

gno_t uuidSetRemove(uuidSet* uuid_set, gno_t start, gno_t end)  {
    if (!gtidIntervalIsValid(start, end)) return 0;
    return gtidIntervalSkipListRemove(uuid_set->intervals,start,end);
}

gno_t uuidSetRaise(uuidSet *uuid_set, gno_t watermark) {
    if (!gtidIntervalIsValid(1, watermark)) return 0;
    return gtidIntervalSkipListAdd(uuid_set->intervals,1,watermark);
}

gno_t uuidSetMerge(uuidSet* dst, uuidSet* src) {
    if (dst->uuid_len != src->uuid_len ||
            memcmp(dst->uuid, src->uuid, src->uuid_len))
        return 0;
    gno_t added = gtidIntervalSkipListMerge(dst->intervals, src->intervals);
    return added;
}

gno_t uuidSetDiff(uuidSet* dst, uuidSet* src) {
    if (dst->uuid_len != src->uuid_len ||
            memcmp(dst->uuid, src->uuid, src->uuid_len))
        return 0;
    return gtidIntervalSkipListDiff(dst->intervals, src->intervals);
}

int uuidSetContains(uuidSet* uuid_set, gno_t gno) {
    if (!gtidIntervalIsValid(1, gno)) return 0;
    return gtidIntervalSkipListContains(uuid_set->intervals, gno);
}

gno_t uuidSetNext(uuidSet* uuid_set, int update) {
    return gtidIntervalSkipListNext(uuid_set->intervals, update);
}

gtidSet* gtidSetNew() {
    gtidSet *gtid_set = gtid_malloc(sizeof(*gtid_set));
    gtid_set->header = NULL;
    gtid_set->tail = NULL;
    gtid_set->current = NULL;
    gtid_set->curnext = 0;
    gtid_set->cached = NULL;
    return gtid_set;
}

void gtidSetFree(gtidSet *gtid_set) {
    if (gtid_set == NULL) return;
    uuidSet *cur = gtid_set->header, *next;
    while(cur != NULL) {
        next = cur->next;
        uuidSetFree(cur);
        cur = next;
    }
    gtid_free(gtid_set);
}

gtidSet* gtidSetDup(gtidSet *gtid_set) {
    gtidSet *result = gtid_malloc(sizeof(gtidSet));
    uuidSet *cur = gtid_set->header, *x = NULL, *p = NULL;
    result->current = NULL;
    result->curnext = 0;
    result->cached = NULL;
    result->header = NULL;
    while (cur) {
        x = uuidSetDup(cur);
        if (p) p->next = x;
        if (!result->header) result->header = x;
        p = x;
        cur = cur->next;
    }
    result->tail = x;
    return result;
}

gno_t gtidSetAppend(gtidSet *gtid_set, uuidSet *uuid_set) {
    if (uuid_set == NULL) return 0;
    if (gtid_set->header == NULL) {
        gtid_set->header = uuid_set;
        gtid_set->tail = uuid_set;
    } else {
        gtid_set->tail->next = uuid_set;
        gtid_set->tail = uuid_set;
    }
    return uuidSetCount(uuid_set);
}

gtidSet *gtidSetDecode(char* src, size_t len) {
    uuidSet *uuid_set;
    gtidSet* gtid_set = gtidSetNew();
    const char *split = ",";
    int uuid_str_start_index = 0;
    if (len == 0) return gtid_set;
    for(size_t i = 0; i < len; i++) {
        if(src[i] == split[0]) {
            uuid_set = uuidSetDecode(src+uuid_str_start_index,
                    i-uuid_str_start_index);
            if (uuid_set == NULL) goto err;
            gtidSetAppend(gtid_set, uuid_set);
            uuid_str_start_index = (i + 1);
        }
    }
    uuid_set = uuidSetDecode(src+uuid_str_start_index,
            len-uuid_str_start_index);
    if (uuid_set == NULL) goto err;
    gtidSetAppend(gtid_set, uuid_set);
    return gtid_set;
err:
    gtidSetFree(gtid_set);
    return NULL;
}

size_t gtidSetEstimatedEncodeBufferSize(gtidSet* gtid_set) {
    size_t max_len = 1;
    uuidSet *cur = gtid_set->header;
    while(cur != NULL) {
        max_len += uuidSetEstimatedEncodeBufferSize(cur) + 1;
        cur = cur->next;
    }
    return max_len;
}


ssize_t gtidSetEncode(char *buf, size_t maxlen, gtidSet* gtid_set) {
    size_t len = 0, ret;
    int first = 1;
    for (uuidSet *cur = gtid_set->header;cur != NULL; cur = cur->next) {
        if (uuidSetCount(cur) == 0) continue;

        if (first) {
            first = 0;
        } else {
            if (len+1 > maxlen) goto err;
            memcpy(buf+len, ",", 1), len += 1;
        }

        ret = uuidSetEncode(buf+len, maxlen-len, cur);
        if (ret < 0) goto err;
        len += ret;
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

gno_t gtidSetAdd(gtidSet* gtid_set, const char* uuid, size_t uuid_len,
        gno_t start, gno_t end) {
    uuidSet *cur = NULL;

    if (gtid_set->cached &&
            gtid_set->cached->uuid_len == uuid_len &&
            memcmp(gtid_set->cached->uuid,uuid,uuid_len) == 0) {
        /* Fast path: adding to cached uuidSet */
        cur = gtid_set->cached;
    } else {
        cur = gtidSetFind(gtid_set, uuid, uuid_len);
    }

    if (cur == NULL) {
        cur = uuidSetNew(uuid, uuid_len);
        gtidSetAppend(gtid_set, cur);
    }
    gtid_set->cached = cur;
    return uuidSetAdd(cur, start, end);
}

gno_t gtidSetRemove(gtidSet* gtid_set, const char *uuid, size_t uuid_len,
        gno_t start, gno_t end) {
    int removed = 0;
    uuidSet *cur = gtid_set->header, *prev = NULL;
    while(cur != NULL) {
        if (cur->uuid_len == uuid_len &&
                memcmp(cur->uuid, uuid, uuid_len) == 0) {
            removed = uuidSetRemove(cur,start,end);
            if (uuidSetCount(cur) == 0) {
                if (gtid_set->current == cur) gtid_set->current = NULL;
                if (prev) prev->next = cur->next;
                if (gtid_set->header == cur) gtid_set->header = cur->next;
                if (gtid_set->tail == cur) gtid_set->tail = prev;
                uuidSetFree(cur);
                cur = NULL;
            }
            break;
        }
        prev = cur;
        cur = cur->next;
    }
    gtid_set->cached = cur;
    return removed;
}

gno_t gtidSetMerge(gtidSet* dst, gtidSet* src) {
    if (src == NULL) return 0;
    gno_t added = 0;
    uuidSet *src_uuid_set = src->header, *dst_uuid_set;
    while(src_uuid_set != NULL) {
        dst_uuid_set = gtidSetFind(dst, src_uuid_set->uuid,
                src_uuid_set->uuid_len);
        if(dst_uuid_set != NULL) {
            added += uuidSetMerge(dst_uuid_set, src_uuid_set);
        } else {
            added += gtidSetAppend(dst, uuidSetDup(src_uuid_set));
        }
        src_uuid_set = src_uuid_set->next;
    }
    return added;
}

gno_t gtidSetDiff(gtidSet* dst, gtidSet* src) {
    if (src == NULL) return 0;
    gno_t removed = 0;
    uuidSet *src_uuid_set, *cur = dst->header, *prev = NULL, *next;
    while(cur != NULL) {
        next = cur->next;

        src_uuid_set = gtidSetFind(src, cur->uuid, cur->uuid_len);
        if(src_uuid_set) removed += uuidSetDiff(cur, src_uuid_set);

        if (uuidSetCount(cur) == 0) {
            /* delete uuid_set if it turns empty */
            if (prev) prev->next = next;
            if (dst->header == cur) dst->header = next;
            if (dst->tail == cur) dst->tail = prev;
            if (dst->cached == cur) dst->cached = NULL;
            uuidSetFree(cur);
        } else {
            prev = cur;
        }
        cur = next;
    }
    return removed;
}

gno_t gtidSetNext(gtidSet* gtid_set, const char* uuid, size_t uuid_len,
        int update) {
    uuidSet *uuid_set = gtidSetFind(gtid_set, uuid, uuid_len);
    if (uuid_set == NULL) {
        if (update) {
            uuid_set = uuidSetNew(uuid, uuid_len);
            gtidSetAppend(gtid_set, uuid_set);
        } else {
            return GTID_GNO_INITIAL;
        }
    }
    gtid_set->cached = uuid_set;
    return uuidSetNext(uuid_set,update);
}

gno_t gtidSetCount(gtidSet *gtid_set) {
    gno_t count = 0;
    for (uuidSet *cur = gtid_set->header; cur != NULL; cur = cur->next) {
        count += uuidSetCount(cur);
    }
    return count;
}

int gtidSetContains(gtidSet* gtid_set, const char* uuid, size_t uuid_len,
        gno_t gno) {
    uuidSet *uuid_set = gtidSetFind(gtid_set, uuid, uuid_len);
    gtid_set->cached = uuid_set;
    return uuid_set != NULL ? uuidSetContains(uuid_set,gno) : 0;
}

void gtidStatMerge(gtidStat *sum, gtidStat *one) {
    sum->uuid_count += one->uuid_count;
    sum->used_memory += one->used_memory;
    sum->gap_count += one->gap_count;
    sum->gno_count += one->gno_count;
}

int gtidSetEqual(gtidSet *set1, gtidSet *set2) {
    gtidSet *set;
    gno_t count;

    set = gtidSetDup(set1);
    gtidSetDiff(set,set2);
    count = gtidSetCount(set);
    gtidSetFree(set);

    if (count) return 0;

    set = gtidSetDup(set2);
    gtidSetDiff(set,set1);
    count = gtidSetCount(set);
    gtidSetFree(set);

    return count ? 0 : 1;
}

/* return 1 if set1 and set2 has common uuid */
int gtidSetRelated(gtidSet *set1, gtidSet *set2) {
    uuidSet *uuid_set = set2->header;
    while (uuid_set) {
        if (gtidSetFind(set1,uuid_set->uuid,uuid_set->uuid_len)) return 1;
        uuid_set = uuid_set->next;
    }
    return 0;
}

void uuidSetGetStat(uuidSet *uuid_set, gtidStat *stat) {
    stat->uuid_count = 1;
    stat->used_memory = uuid_set->intervals->node_count * GTID_INTERVAL_MEMORY;
    stat->gap_count = uuid_set->intervals->node_count-1;
    stat->gno_count = uuid_set->intervals->gno_count;
}

void gtidSetGetStat(gtidSet *gtid_set, gtidStat *stat) {
    uuidSet *uuid_set = gtid_set->header;
    memset(stat,0,sizeof(*stat));
    while (uuid_set) {
        gtidStat uuid_stat;
        uuidSetGetStat(uuid_set, &uuid_stat);
        gtidStatMerge(stat, &uuid_stat);
        uuid_set = uuid_set->next;
    }
}

/* Note that empty uuidSet might be added if needed */
void gtidSetCurrentUuidSetUpdate(gtidSet *gtid_set, const char *uuid,
        size_t uuid_len) {
    uuidSet *uuid_set = gtidSetFind(gtid_set,uuid,uuid_len);
    if (uuid_set == NULL) {
        uuid_set = uuidSetNew(uuid,uuid_len);
        gtidSetAppend(gtid_set,uuid_set);
    }
    gtid_set->current = uuid_set;
}

const char *gtidAllocatorName() {
    return GTID_ALLOC_LIB;
}

gtidSegment *gtidSegmentNew() {
    gtidSegment *seg = gtid_malloc(sizeof(gtidSegment));
    memset(seg,0,sizeof(gtidSegment));
    seg->capacity = GTID_SEGMENT_NGNO_DEFAULT;
    seg->deltas = gtid_malloc(sizeof(segoff_t)*GTID_SEGMENT_NGNO_DEFAULT);
    return seg;
}

void gtidSegmentFree(gtidSegment *seg) {
    if (seg == NULL) return;
    if (seg->uuid) {
        gtid_free(seg->uuid);
        seg->uuid = NULL;
    }
    if (seg->deltas) {
        gtid_free(seg->deltas);
        seg->deltas = NULL;
    }
    gtid_free(seg);
}

void gtidSegmentReset(gtidSegment *seg, const char *uuid,
        size_t uuid_len, gno_t base_gno, long long base_offset) {
    if (seg->uuid_len != uuid_len || memcmp(seg->uuid, uuid, uuid_len)) {
        if (seg->uuid) gtid_free(seg->uuid);
        uuidDup(&seg->uuid,&seg->uuid_len,uuid,uuid_len);
    }
    seg->base_offset = base_offset;
    seg->base_gno = base_gno;
    seg->tgno = 0;
    seg->ngno = 0;
}

void gtidSegmentAppend(gtidSegment *seg, long long offset) {
    size_t delta = offset - seg->base_offset;
    assert(delta >= 0 && delta <= SEGOFF_MAX);
    assert(seg->ngno <= seg->capacity);
    if (seg->ngno == seg->capacity) {
        seg->capacity *= 2;
        seg->deltas = gtid_realloc(seg->deltas,sizeof(segoff_t)*seg->capacity);
    }
    seg->deltas[seg->ngno++] = delta;
}

gtidSeq *gtidSeqCreate() {
    gtidSeq *seq = gtid_malloc(sizeof(struct gtidSeq));
    seq->segment_size = SEGMENT_SIZE;
    seq->nsegment = 0;
    seq->nfreeseg = 0;
    seq->nsegment_deltas = 0;
    seq->nfreeseg_deltas = 0;
    seq->firstseg = NULL;
    seq->lastseg = NULL;
    seq->freeseg = NULL;
    return seq;
}

void gtidSeqDestroy(gtidSeq *seq) {
    gtidSegment *seg, *next;

    if (seq == NULL) return;

    seg = seq->firstseg;
    while (seg) {
        next = seg->next;
        seq->nsegment--;
        seq->nsegment_deltas -= seg->capacity;
        gtidSegmentFree(seg);
        seg = next;
    }
    seq->firstseg = NULL;
    seq->lastseg = NULL;
    assert(seq->nsegment == 0 && seq->nsegment_deltas == 0);

    seg = seq->freeseg;
    while (seg) {
        next = seg->next;
        seq->nfreeseg--;
        seq->nfreeseg_deltas -= seg->capacity;
        gtidSegmentFree(seg);
        seg = next;
    }
    seq->freeseg = NULL;
    assert(seq->nfreeseg == 0 && seq->nfreeseg_deltas == 0);

    gtid_free(seq);
}


void gtidSeqRebaseOffset(gtidSeq *seq, const char *uuid, size_t uuid_len, size_t offset) {
    gtidSegment *seg = seq->firstseg;
    while (seg) {
        seg->base_offset += offset;
        seg = seg->next;
    }
}

static inline
gtidSegment *gtidSeqSwitchSegment(gtidSeq *seq, const char *uuid,
        size_t uuid_len, gno_t base_gno, long long base_offset) {
    gtidSegment *seg = NULL;

    if (seq->freeseg) {
        /* unlink seg from free list */
        seg = seq->freeseg;
        seq->freeseg = seg->next;
        assert(seg->prev == NULL);
        seg->next = NULL;
        seq->nfreeseg--;
        seq->nfreeseg_deltas -= seg->capacity;
    } else {
        /* create new seg */
        seg = gtidSegmentNew();
    }

    gtidSegmentReset(seg,uuid,uuid_len,base_gno,base_offset);

    seg->prev = seq->lastseg;
    if (!seq->firstseg) seq->firstseg = seg;
    if (seq->lastseg) seq->lastseg->next = seg;
    seq->lastseg = seg;
    seq->nsegment++;
    seq->nsegment_deltas += seg->capacity;

    return seg;
}

void gtidSeqAppend(gtidSeq *seq, const char *uuid, size_t uuid_len,
        gno_t gno, long long offset) {
    gtidSegment *lastseg = seq->lastseg;

    if (lastseg) {
        long long tail_offset;
        assert(lastseg->ngno > 0);
        tail_offset = lastseg->base_offset + lastseg->deltas[lastseg->ngno-1];
        assert(tail_offset < offset);
    }
    if(lastseg == NULL /* no previous segment */ ||
            lastseg->uuid_len != uuid_len /* uuid switch */ ||
            memcmp(lastseg->uuid, uuid, uuid_len) /* uuid switch */ ||
            lastseg->base_gno + (gno_t)lastseg->ngno != gno /* gno gap */ ||
            lastseg->base_offset + (long long)seq->segment_size <= offset) {
        lastseg = gtidSeqSwitchSegment(seq,uuid,uuid_len,gno,offset);
    }

    size_t prev_capacity = lastseg->capacity;
    gtidSegmentAppend(lastseg,offset);
    seq->nsegment_deltas += lastseg->capacity - prev_capacity;
}

void gtidSeqTrim(gtidSeq *seq, long long until) {
    while (seq->firstseg) {
        long long tail_offset;
        gtidSegment *seg = seq->firstseg;

        /* no empty segment allowed */
        assert(seg->ngno > seg->tgno);

        tail_offset = seg->base_offset + seg->deltas[seg->ngno-1];

        if (tail_offset < until) { /* whole segment trimmed */
            seq->nsegment--;
            seq->nsegment_deltas -= seg->capacity;
            seq->firstseg = seg->next;
            if (seq->firstseg) seq->firstseg->prev = NULL;
            if (!seq->firstseg) seq->lastseg = NULL;

            if (seq->nsegment > seq->nfreeseg) {
                /* cache no more than used segment */
                seg->next = seq->freeseg, seg->prev = NULL;
                seq->freeseg = seg;
                seq->nfreeseg++;
                seq->nfreeseg_deltas += seg->capacity;
            } else {
                gtidSegmentFree(seg);
            }
        } else { /* at least last gno will be kept */
            long long offset;
            size_t l = seg->tgno, r = seg->ngno, m;

            while (l < r) {
                m = l + (r-l)/2;
                offset = seg->base_offset + seg->deltas[m];
                if (offset < until) {
                    l = m+1;
                } else {
                    r = m;
                }
            }

            /* l is the first segment that should be kept */
            seg->tgno = l;
            assert(seg->ngno > seg->tgno);
            break;
        }
    }
}

/* <uuid>:[<gno_1>=<offset_1>,<gno_2>=<offset_2>,...] */
static inline size_t gtidSegmentEstimatedEncodeBufferSize(gtidSegment *seg) {
    size_t len = 0;
    len += seg->uuid_len + 1; /* uuid: */
    len += 2; /* [] */
    len += (seg->ngno-seg->tgno)*(20+1+20+1); /* <gno_i>=<offset_i>, */
    return len;
}

static inline size_t gtidSegmentEncode(char *buf, size_t maxlen, gtidSegment *seg) {
    size_t len = 0;
    len += snprintf(buf+len,maxlen-len,"%.*s:[",(int)seg->uuid_len,seg->uuid);
    for (size_t i = seg->tgno; i < seg->ngno; i++) {
        len += snprintf(buf+len,maxlen-len,"%llu=%llu,",
                seg->base_gno+i,seg->base_offset+seg->deltas[i]);
    }
    len += snprintf(buf+len,maxlen-len,"]");
    return len;
}

size_t gtidSeqEstimatedEncodeBufferSize(gtidSeq* seq) {
    size_t len = 0;
    if (seq->firstseg) {
        len += gtidSegmentEstimatedEncodeBufferSize(seq->firstseg);
    }
    if (seq->lastseg != seq->firstseg) {
        len += 5; /* ;...; */
        len += gtidSegmentEstimatedEncodeBufferSize(seq->lastseg);
    }
    return len;
}

/* <first_seg>;...;<last_seg>*/
ssize_t gtidSeqEncode(char *buf, size_t maxlen, gtidSeq* seq) {
    size_t len = 0;
    if (seq->firstseg) {
        len += gtidSegmentEncode(buf+len,maxlen-len,seq->firstseg);
    }
    if (seq->lastseg != seq->firstseg) {
        len += snprintf(buf+len,maxlen-len,";...;");
        len +=gtidSegmentEncode(buf+len,maxlen-len,seq->lastseg);
    }
    return len;
}

/* Locate xsync continue position, return continue offset and gitset from
 * continue to end. */
long long gtidSeqXsync(gtidSeq *seq, gtidSet *req, gtidSet **pcont) {
    long long offset = -1;
    gtidSegment *seg = seq->lastseg;
    gtidSet *cont = gtidSetNew();
    // printf("[latte] gtidSeqXsync seg==NULL %d\n", seg);
    while (seg) {
        uuidSet *uuid_set = gtidSetFind(req,seg->uuid,seg->uuid_len);
        gno_t next_gno = uuid_set ? uuidSetNext(uuid_set,0) : GTID_GNO_INITIAL;
        gno_t start_gno = seg->base_gno + seg->tgno;
        gno_t end_gno = seg->base_gno + seg->ngno - 1; /* inclusive */
        
        if (next_gno > end_gno) {
            seg = NULL;
        } else if (next_gno > start_gno) {
            offset = seg->base_offset + seg->deltas[next_gno - seg->base_gno];
            gtidSetAdd(cont,seg->uuid,seg->uuid_len,next_gno,end_gno);
            seg = NULL;
        } else {
            offset = seg->base_offset + seg->deltas[start_gno - seg->base_gno];
            gtidSetAdd(cont,seg->uuid,seg->uuid_len,start_gno,end_gno);
            seg = seg->prev;
        }
    }

    if (pcont) {
        *pcont = cont;
    } else {
        gtidSetFree(cont);
    }

    return offset;
}

gtidSet *gtidSeqPsync(gtidSeq *seq, long long offset) {
    gtidSegment *seg = seq->lastseg;
    gtidSet *gtid_set = gtidSetNew();

    while (seg) {
        gno_t start_gno, end_gno;
        long long start_offset = seg->base_offset + seg->deltas[seg->tgno];

        if (start_offset >= offset) {
            start_gno = seg->base_gno + seg->tgno;
            end_gno = seg->base_gno + seg->ngno - 1; /* inclusive */
            gtidSetAdd(gtid_set,seg->uuid,seg->uuid_len,start_gno,end_gno);
            seg = seg->prev;
        } else {
            long long moffset;
            size_t l = seg->tgno, r = seg->ngno, m;
            while (l < r) {
                m = (l + r)/2;
                moffset = seg->base_offset + seg->deltas[m];
                if (moffset < offset) {
                    l = m+1;
                } else {
                    r = m;
                }
            }
            start_gno = seg->base_gno + l;
            end_gno = seg->base_gno + seg->ngno - 1; /* inclusive */
            gtidSetAdd(gtid_set,seg->uuid,seg->uuid_len,start_gno,end_gno);
            seg = NULL;
        }
    }

    return gtid_set;
}

void gtidSeqGetStat(gtidSeq *seq, gtidSeqStat *stat) {
    stat->segment_memory = seq->nsegment*sizeof(gtidSegment) +
        seq->nsegment_deltas*sizeof(segoff_t);
    stat->freeseg_memory = seq->nfreeseg*sizeof(gtidSegment) +
        seq->nfreeseg_deltas*sizeof(segoff_t);
    stat->used_memory = stat->segment_memory + stat->freeseg_memory;
}
