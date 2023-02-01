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

#include <stdio.h>
#include <string.h>
#include <limits.h>
#include "util.h"
#include "gtid.h"

#ifndef GTID_MALLOC_INCLUDE
#define GTID_MALLOC_INCLUDE "gtid_malloc.h"
#endif

#include GTID_MALLOC_INCLUDE


char* sidDup(const char* sid, int sid_len) {
	char* dup = gtid_malloc(sid_len + 1);
	memcpy(dup, sid, sid_len);
	dup[sid_len] = '\0';
	return dup;
}

gtidInterval *gtidIntervalNew(gno_t gno) {
    return gtidIntervalNewRange(gno, gno);
}

gtidInterval *gtidIntervalDup(gtidInterval* interval) {
    gtidInterval* dup = gtidIntervalNewRange(interval->start, interval->end);
    if(interval->next != NULL) {
        dup->next = gtidIntervalDup(interval->next);
    }
    return dup;
}

void gtidIntervalFree(gtidInterval* interval) {
    gtid_free(interval);
}

gtidInterval *gtidIntervalNewRange(gno_t start, gno_t end) {
    gtidInterval *interval = gtid_malloc(sizeof(*interval));
    interval->start = start;
    interval->end = end;
    interval->next = NULL;
    return interval;
}

gtidInterval *gtidIntervalDecode(char* interval_str, size_t len) {
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
            return gtidIntervalNew(start);
        }
    } else {
        //{start}-{end}
        if(string2ll(interval_str, index, &start) &&
         string2ll(interval_str + index + 1, len - index - 1, &end)) {
             return gtidIntervalNewRange(start, end);
        }
    }
    return NULL;
}

size_t gtidIntervalEncode(gtidInterval* interval, char* buf) {
    size_t len = 0;
    len += ll2string(buf + len, 21, interval->start);
    if(interval->start != interval->end) {
        memcpy(buf + len, "-", 1), len += 1;
        len += ll2string(buf + len, 21, interval->end);
    }
    return len;
}


uuidSet *uuidSetNew(const char *sid, size_t sid_len, gno_t gno) {
    return uuidSetNewRange(sid, sid_len, gno, gno);
}

uuidSet *uuidSetNewRange(const char *sid, size_t sid_len, gno_t start, gno_t end) {
    uuidSet *uuid_set = gtid_malloc(sizeof(*uuid_set));
    uuid_set->sid = sidDup(sid,sid_len);
    uuid_set->sid_len = sid_len;
    uuid_set->intervals = gtidIntervalNewRange(start, end);
    uuid_set->next = NULL;
    return uuid_set;
}

void uuidSetFree(uuidSet* uuid_set) {
    gtid_free(uuid_set->sid);
    gtidInterval *cur = uuid_set->intervals;
    while(cur != NULL) {
        gtidInterval *next = cur->next;
        gtid_free(cur);
        cur = next;
    }
    gtid_free(uuid_set);
}

uuidSet *uuidSetDup(uuidSet* uuid_set) {
    uuidSet* dup = uuidSetNewRange(uuid_set->sid, uuid_set->sid_len, uuid_set->intervals->start, uuid_set->intervals->end);
    if(uuid_set->intervals->next != NULL) {
        dup->intervals->next = gtidIntervalDup(uuid_set->intervals->next);
    }
    if(uuid_set->next != NULL) {
        dup->next = uuidSetDup(uuid_set->next);
    }
    return dup;
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
            if(uuid_set == NULL) {
                uuid_set = gtid_malloc(sizeof(*uuid_set));
                uuid_set->sid = sidDup(uuid_set_str,i);
                uuid_set->sid_len = i;
                uuid_set->intervals = NULL;
                uuid_set->next = NULL;
                start_index = i + 1;
                break;
            }
        }
    }
    if(uuid_set == NULL) {
        return NULL;
    }
    int end_index = len - 1;
    int count = 0;
    for(int i = len - 2; i >= start_index; i--) {
        if(uuid_set_str[i] == colon[0]) {
            if(i == end_index) {
                goto ERROR;
            }
            gtidInterval* interval = gtidIntervalDecode(uuid_set_str + i + 1, end_index - i);
            interval->next = uuid_set->intervals;
            uuid_set->intervals = interval;
            end_index = i - 1;
            count++;
        }
    }
    gtidInterval* interval = gtidIntervalDecode(uuid_set_str + start_index, end_index - start_index + 1);
    interval->next = uuid_set->intervals;
    uuid_set->intervals = interval;
    return uuid_set;
    ERROR:
        if(uuid_set != NULL) {
            uuidSetFree(uuid_set);
            uuid_set = NULL;
        }
        return NULL;
}

size_t uuidSetEstimatedEncodeBufferSize(uuidSet* uuid_set) {
    //{sid}: {longlong}-{longlong}* n
    size_t intervals_count = 0;
    gtidInterval* current = uuid_set->intervals;
    while(current != NULL) {
        intervals_count++;
        current = current->next;
    }
    return uuid_set->sid_len + intervals_count * 44; // 44 = 1(:) + 21(longlong) + 1(-) + 21(long long)
}

size_t uuidSetEncode(uuidSet* uuid_set, char* buf) {
    size_t len = 0;
    memcpy(buf + len, uuid_set->sid, uuid_set->sid_len), len += uuid_set->sid_len;
    gtidInterval *cur = uuid_set->intervals;
    while(cur != NULL) {
        memcpy(buf + len, ":", 1), len += 1;
        len += gtidIntervalEncode(cur, buf + len);
        cur = cur->next;
    }
    return len;
}

#define min(a, b)	(a) < (b) ? a : b
#define max(a, b)	(a) < (b) ? b : a
int uuidSetAddGtidInterval(uuidSet* uuid_set, gtidInterval* interval) {
    gtidInterval *cur = uuid_set->intervals;
    gtidInterval *next = cur->next;
    /* A:0  +  A:1-10  = A:1-10 */
    if (cur->start == 0 && cur->end == 0) {
        cur->start = interval->start;
        cur->end = interval->end;
        return 1;
    }
    /* A:4-5:7-8:10-11  + A:1-2 = A:1-2:4-5:7-8:10-11 */
    if (interval->end < cur->start - 1) {
        uuid_set->intervals = gtidIntervalDup(interval);
        uuid_set->intervals->next = cur;
        return 1;
    }
    int changed = 0;
    char* error_scope;
    do {
        //  A  {cur->start} B {cur->end} C {next->start} D {next->end} E
        //  next B = D
        if(interval->start < cur->start - 1) {
            if(interval->end < cur->start - 1) {
                //A-A
                //ignore
                //exec C-C
                error_scope = "A-A";
                goto Error;
            } else if(interval->end <= cur->end + 1){
                //A-B
                cur->start = interval->start;
                cur->end = max(interval->end, cur->end);
                return 1;
            } else if(next == NULL || (next != NULL && interval->end < next->start - 1)) {
                //A-C
                cur->start = interval->start;
                cur->end = interval->end;
                return 1;
            } else if(interval->end <= next->end + 1) {
                //A-D
                cur->start = interval->start;
                cur->end = max(next->end, interval->end);
                cur->next = next->next;
                gtidIntervalFree(next);
                return 1;
            } else {
                //A-E
                cur->end = next->end;
                cur->next = next->next;
                gtidIntervalFree(next);
                next = cur->next;
                changed = 1;
                continue;
            }

        } else if(interval->start <= cur->end + 1) {
            //B
            if(interval->end < cur->start - 1) {
                //B-A
                //ignore
                error_scope = "B-A";
                goto Error;
            } else if(interval->end <= cur->end + 1){
                //B-B
                //ignore
                long long start_min = min(interval->start, cur->start);
                long long end_max = max(interval->end, cur->end);
                if (start_min != cur->start || end_max != cur->end) {
                    changed = 1;
                }
                cur->start = start_min;
                cur->end = end_max;
                if(next != NULL && cur->end == next->start - 1) {
                    cur->end = next->end;
                    cur->next = next->next;
                    gtidIntervalFree(next);
                }
                return changed;
            } else if(next == NULL || interval->end < next->start - 1) {
                //B-C
                cur->start = min(interval->start, cur->start);
                cur->end = interval->end;
                return 1;
            } else if(interval->end <= next->end + 1){
                //B-D
                cur->start = min(interval->start, cur->start);
                cur->end = max(next->end, interval->end);
                cur->next = next->next;
                gtidIntervalFree(next);
                return 1;
            } else {
                //B-E
                cur->start = min(interval->start, cur->start);
                cur->end = next->end;
                cur->next = next->next;
                gtidIntervalFree(next);
                next = cur->next;
                changed = 1;
                continue;
            }
        } else if(next == NULL || interval->end < next->start - 1) {
            //C
            if(interval->end < cur->start - 1) {
                //ignore
                //C-A
                error_scope = "C-A";
                goto Error;
            } else if(interval->end <= cur->end + 1){
                //C-B
                error_scope = "C-B";
                goto Error;
            } else if(next == NULL || interval->end < next->start - 1) {
                //C-C
                gtidInterval* new_next = gtidIntervalDup(interval);
                new_next->next = cur->next;
                cur->next = new_next;
                return 1;
            } else if(interval->end <= next->end + 1){
                //C-D
                next->start = cur->start;
                return 1;
            } else {
                //C-E
                next->start = cur->start;
                changed = 1;
            }
        } else {
            //ignore
            // 1. start <= end   =>   B * A = 0(not exist)
            //     （A,B) * (A,B) = A* A + A* B + B*B
            // 2. D = (next B), E = (next C)
            // 3. (A, B, C, D ,E) * (A, B, C, D ,E)
            //        = (A,B,C) * (A,B,C,D,E)  + D * D + D * E + E * E
            //        = (A,B,C) * (A,B,C,D,E)  + next (B * B) + next (B * C) + next(C * C)
            //        <  (A,B,C) * (A,B,C,D,E)  + next ((A,B,C) * (A,B,C,D,E))

        }

        cur = next;
        if (cur!= NULL) {
            next = cur->next;
        }
    }while(cur != NULL);
Error:
    printf("\n code error [%s] %lld-%lld",error_scope, interval->start, interval->end);
    printf("cur %lld-%lld\n", cur->start, cur->end);
    if(next != NULL) {
        printf("next %lld-%lld\n", next->start, next->end);
    }
    exit(0);
}

int uuidSetAdd(uuidSet* uuid_set, gno_t gno)  {
    gtidInterval interval = {
        .start = gno,
        .end = gno,
        .next = NULL
    };
    return uuidSetAddGtidInterval(uuid_set, &interval);
}

void uuidSetRaise(uuidSet *uuid_set, gno_t watermark) {
    gtidInterval *cur = uuid_set->intervals;
    if (watermark < cur->start - 1) {
        uuid_set->intervals = gtidIntervalNewRange(1, watermark);
        uuid_set->intervals->next = cur;
        return;
    }

    while (cur != NULL) {
        if (watermark > cur->end + 1) {
            gtidInterval *temp = cur;
            cur = cur->next;
            gtid_free(temp);
            continue;
        }

        if (watermark == cur->end + 1) {
            if (cur->next == NULL) {
                cur->start = 1;
                cur->end = watermark;
                uuid_set->intervals = cur;
                break;
            }
            if (watermark == cur->next->start - 1) {
                gtidInterval *prev = cur;
                cur = cur->next;
                gtid_free(prev);
                cur->start = 1;
                break;
            } else {
                cur->end = watermark;
                cur->start = 1;
                break;
            }
        }
        if (watermark < cur->start - 1) {
            gtidInterval *temp = cur;
            cur = gtidIntervalNewRange(1, watermark);
            cur->next = temp;
            break;
        } else {
            cur->start = 1;
            break;
        }
    }
    if (cur == NULL) {
        uuid_set->intervals = gtidIntervalNewRange(1, watermark);
    } else {
        uuid_set->intervals = cur;
    }
}

int uuidSetContains(uuidSet* uuid_set, gno_t gno) {
    gtidInterval *cur = uuid_set->intervals;
    while (cur != NULL) {
        if (gno >= cur->start && gno <= cur->end) {
            return 1;
        }
        cur = cur->next;
    }
    return 0;
}

gno_t uuidSetNext(uuidSet* uuid_set, int updateBeforeReturn) {
    if (uuid_set->intervals == NULL) {
        if (updateBeforeReturn) {
            uuid_set->intervals = gtidIntervalNew(1);
        }
        return 1;
    }

    gno_t next;
    if (uuid_set->intervals->start != 1) {
        next = 1;
    } else {
        next = uuid_set->intervals->end + 1;
    }
    if (updateBeforeReturn) {
        uuidSetAdd(uuid_set, next);
    }
    return next;
}

size_t uuidSetNextEncode(uuidSet* uuid_set, int updateBeforeReturn, char* buf) {
    size_t len = 0;
    memcpy(buf + len, uuid_set->sid, uuid_set->sid_len), len += uuid_set->sid_len;
    gno_t gno = uuidSetNext(uuid_set, updateBeforeReturn);
    memcpy(buf + len, ":", 1), len += 1;
    len += ll2string(buf + len, 21, gno);
    return len;
}

int uuidSetAppendUuidSet(uuidSet* uuid_set, uuidSet* other) {
    if(strcmp(uuid_set->sid, other->sid) != 0) {
        return 0;
    }
    gtidInterval* interval =  other->intervals;
    while(interval != NULL) {
        uuidSetAddGtidInterval(uuid_set, interval);
        interval = interval->next;
    }
    return 1;
}

gtidSet* gtidSetNew() {
    gtidSet *gtid_set = gtid_malloc(sizeof(*gtid_set));
    gtid_set->uuid_sets = NULL;
    gtid_set->tail = NULL;
    return gtid_set;
}

void gtidSetFree(gtidSet *gtid_set) {
    uuidSet *next;
    uuidSet *cur = gtid_set->uuid_sets;
    while(cur != NULL) {
        next = cur->next;
        uuidSetFree(cur);
        cur = next;
    }
    gtid_free(gtid_set);
}

void gtidSetAppendUuidSet(gtidSet *gtid_set, uuidSet *uuid_set) {
    if (gtid_set->uuid_sets == NULL) {
        gtid_set->uuid_sets = uuid_set;
        gtid_set->tail = uuid_set;
    } else {
        gtid_set->tail->next = uuid_set;
        gtid_set->tail = uuid_set;
    }
}

gtidSet *gtidSetDecode(char* src, size_t len) {
    gtidSet* gtid_set = gtidSetNew();
    const char *split = ",";
    int uuid_str_start_index = 0;
    for(int i = 0; i < len; i++) {
        if(src[i] == split[0]) {
            uuidSet *uuid_set = uuidSetDecode(src + uuid_str_start_index, i - uuid_str_start_index);
            gtidSetAppendUuidSet(gtid_set, uuid_set);
            uuid_str_start_index = (i + 1);
        }
    }
    uuidSet *uuid_set = uuidSetDecode(src + uuid_str_start_index, len - uuid_str_start_index);
    gtidSetAppendUuidSet(gtid_set, uuid_set);
    return gtid_set;
}


size_t gtidSetEstimatedEncodeBufferSize(gtidSet* gtid_set) {
    size_t max_len = 1; // must > 0;
    uuidSet *cur = gtid_set->uuid_sets;
    while(cur != NULL) {
        max_len += uuidSetEstimatedEncodeBufferSize(cur) + 1;
        cur = cur->next;
    }
    return max_len;
}

size_t gtidSetEncode(gtidSet* gtid_set, char* buf) {
    size_t len = 0;
    uuidSet *cur = gtid_set->uuid_sets;
    while(cur != NULL) {
        len += uuidSetEncode(cur, buf + len);
        cur = cur->next;
        if (cur != NULL) {
            memcpy(buf + len, ",", 1), len += 1;
        }
    }
    return len;
}

uuidSet* gtidSetFindUuidSet(gtidSet* gtid_set, const char* sid, size_t sid_len) {
    uuidSet *cur = gtid_set->uuid_sets;
    while(cur != NULL) {
        if (strncmp(cur->sid, sid, sid_len) == 0) {
            break;
        }
        cur = cur->next;
    }
    return cur;
}

int gtidSetAdd(gtidSet* gtid_set, const char* sid, size_t sid_len ,gno_t gno) {
    uuidSet *cur = gtidSetFindUuidSet(gtid_set, sid, sid_len);
    if (cur == NULL) {
        cur = uuidSetNew(sid, sid_len, gno);
        gtidSetAppendUuidSet(gtid_set, cur);
        return 1;
    } else {
        return uuidSetAdd(cur, gno);
    }
}

char* uuidDecode(char* src, size_t src_len, long long* gno, int* sid_len) {
    const char *split = ":";
    int index = -1;
    for(int i = 0; i < src_len; i++) {
        if(src[i] == split[0]) {
            index = i;
            break;
        }
    }
    if(index == -1) {
        return NULL;
    }
    if(string2ll(src + index + 1, src_len - index - 1, gno) == 0) {
        return NULL;
    }
    *sid_len = index;
    return src;
}

void gtidSetRaise(gtidSet* gtid_set, const char* sid, size_t sid_len, gno_t watermark) {
    if (watermark == 0) return;
    uuidSet *cur = gtidSetFindUuidSet(gtid_set, sid, sid_len);
    if (cur == NULL) {
        cur = uuidSetNewRange(sid, sid_len, 1, watermark);
        gtidSetAppendUuidSet(gtid_set, cur);
    } else {
        uuidSetRaise(cur, watermark);
    }
}

void gtidSetAppendGtidSet(gtidSet* gtid_set, gtidSet* other) {
    uuidSet *cur = other->uuid_sets;
    while(cur != NULL) {
        uuidSet* src = gtidSetFindUuidSet(gtid_set, cur->sid, cur->sid_len);
        if(src != NULL) {
            uuidSetAppendUuidSet(src, cur);
        } else {
            gtidSetAppendUuidSet(gtid_set, uuidSetDup(cur));
        }
        cur = cur->next;
    }
}
