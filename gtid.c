#include <stdio.h>
#include <string.h>
#include <limits.h>
#include "util.h"
#include "gtid.h"
#include "gtid_malloc.h"

/* util api */
size_t writeBuf(char* buf, const char* src, size_t len) {
    memcpy(buf, src, len);
    return len;
}

char* stringNew(const char* src, int len, int max) {
    char* str = gtid_malloc(max + 1);
    writeBuf(str, src, len);
    str[len] = '\0';
    return str;
}

void stringFree(char* str) {
    gtid_free(str);
}

gtidInterval *gtidIntervalNew(rpl_gno gno) {
    return gtidIntervalNewRange(gno, gno);
}

gtidInterval *gtidIntervalDump(gtidInterval* gtid_interval) {
    gtidInterval* dump = gtidIntervalNewRange(gtid_interval->gno_start, gtid_interval->gno_end);
    if(gtid_interval->next != NULL) {
        dump->next = gtidIntervalDump(gtid_interval->next);
    }
    return dump;
}

void gtidIntervalFree(gtidInterval* interval) {
    gtid_free(interval);
}

gtidInterval *gtidIntervalNewRange(rpl_gno start, rpl_gno end) {
    gtidInterval *interval = gtid_malloc(sizeof(*interval));
    interval->gno_start = start;
    interval->gno_end = end;
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
    rpl_gno gno_start = 0, gno_end = 0;
    if(index == -1) {
        if(string2ll(interval_str, len, &gno_start)) {
            return gtidIntervalNew(gno_start);
        }
    } else {
        //{gno_start}-{gno_end}
        if(string2ll(interval_str, index, &gno_start) &&
         string2ll(interval_str + index + 1, len - index - 1, &gno_end)) {
             return gtidIntervalNewRange(gno_start, gno_end);
        }
    }
    return NULL;
}

size_t gtidIntervalEncode(gtidInterval* interval, char* buf) {
    size_t len = 0;
    len += ll2string(buf + len, 21, interval->gno_start);
    if(interval->gno_start != interval->gno_end) {
        len += writeBuf(buf + len, "-", 1);
        len += ll2string(buf + len, 21, interval->gno_end);
    }
    return len;
}


uuidSet *uuidSetNew(const char *rpl_sid, size_t rpl_sid_len, rpl_gno gno) {
    return uuidSetNewRange(rpl_sid, rpl_sid_len, gno, gno);
}

uuidSet *uuidSetNewRange(const char *rpl_sid, size_t rpl_sid_len, rpl_gno start, rpl_gno end) {
    uuidSet *uuid_set = gtid_malloc(sizeof(*uuid_set));
    uuid_set->rpl_sid = stringNew(rpl_sid, rpl_sid_len, rpl_sid_len);
    uuid_set->intervals = gtidIntervalNewRange(start, end);
    uuid_set->next = NULL;
    return uuid_set;
}

void uuidSetFree(uuidSet* uuid_set) {
    stringFree(uuid_set->rpl_sid);
    gtidInterval *cur = uuid_set->intervals;
    while(cur != NULL) {
        gtidInterval *next = cur->next;
        gtid_free(cur);
        cur = next;
    }
    gtid_free(uuid_set);
}

uuidSet *uuidSetDump(uuidSet* uuid_set) {
    uuidSet* dump = uuidSetNewRange(uuid_set->rpl_sid, strlen(uuid_set->rpl_sid), uuid_set->intervals->gno_start, uuid_set->intervals->gno_end);
    if(uuid_set->intervals->next != NULL) {
        dump->intervals->next = gtidIntervalDump(uuid_set->intervals->next);
    }
    if(uuid_set->next != NULL) {
        dump->next = uuidSetDump(uuid_set->next);
    }
    return dump;
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
                uuid_set->rpl_sid = stringNew(uuid_set_str, i, i);
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
    //{rpl_sid}: {longlong}-{longlong}* n
    size_t intervals_count = 0;
    gtidInterval* current = uuid_set->intervals;
    while(current != NULL) {
        intervals_count++;
        current = current->next;
    }
    return strlen(uuid_set->rpl_sid) + intervals_count * 44; // 44 = 1(:) + 21(longlong) + 1(-) + 21(long long)
}

size_t uuidSetEncode(uuidSet* uuid_set, char* buf) {
    size_t len = 0;
    len += writeBuf(buf + len, uuid_set->rpl_sid, strlen(uuid_set->rpl_sid));
    gtidInterval *cur = uuid_set->intervals;
    while(cur != NULL) {
        len += writeBuf(buf + len, ":", 1);
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
    if (cur->gno_start == 0 && cur->gno_end == 0) {
        cur->gno_start = interval->gno_start;
        cur->gno_end = interval->gno_end;
        return 1;
    }
    /* A:4-5:7-8:10-11  + A:1-2 = A:1-2:4-5:7-8:10-11 */
    if (interval->gno_end < cur->gno_start - 1) {
        uuid_set->intervals = gtidIntervalDump(interval);
        uuid_set->intervals->next = cur;
        return 1;
    }
    int changed = 0;
    char* error_scope;
    do {
        //  A  {cur->gno_start} B {cur->gno_end} C {next->gno_start} D {next->gno_end} E
        //  next B = D
        if(interval->gno_start < cur->gno_start - 1) {
            if(interval->gno_end < cur->gno_start - 1) {
                //A-A
                //ignore
                //exec C-C
                error_scope = "A-A";
                goto Error;
            } else if(interval->gno_end <= cur->gno_end + 1){
                //A-B
                cur->gno_start = interval->gno_start;
                cur->gno_end = max(interval->gno_end, cur->gno_end);
                return 1;
            } else if(next == NULL || (next != NULL && interval->gno_end < next->gno_start - 1)) {
                //A-C
                cur->gno_start = interval->gno_start;
                cur->gno_end = interval->gno_end;
                return 1;
            } else if(interval->gno_end <= next->gno_end + 1) {
                //A-D
                cur->gno_start = interval->gno_start;
                cur->gno_end = max(next->gno_end, interval->gno_end);
                cur->next = next->next;
                gtidIntervalFree(next);
                return 1;
            } else {
                //A-E
                cur->gno_end = next->gno_end;
                cur->next = next->next;
                gtidIntervalFree(next);
                next = cur->next;
                changed = 1;
                continue;
            }

        } else if(interval->gno_start <= cur->gno_end + 1) {
            //B
            if(interval->gno_end < cur->gno_start - 1) {
                //B-A
                //ignore
                error_scope = "B-A";
                goto Error;
            } else if(interval->gno_end <= cur->gno_end + 1){
                //B-B
                //ignore
                long long start_min = min(interval->gno_start, cur->gno_start);
                long long end_max = max(interval->gno_end, cur->gno_end);
                if (start_min != cur->gno_start || end_max != cur->gno_end) {
                    changed = 1;
                }
                cur->gno_start = start_min;
                cur->gno_end = end_max;
                if(next != NULL && cur->gno_end == next->gno_start - 1) {
                    cur->gno_end = next->gno_end;
                    cur->next = next->next;
                    gtidIntervalFree(next);
                }
                return changed;
            } else if(next == NULL || interval->gno_end < next->gno_start - 1) {
                //B-C
                cur->gno_start = min(interval->gno_start, cur->gno_start);
                cur->gno_end = interval->gno_end;
                return 1;
            } else if(interval->gno_end <= next->gno_end + 1){
                //B-D
                cur->gno_start = min(interval->gno_start, cur->gno_start);
                cur->gno_end = max(next->gno_end, interval->gno_end);
                cur->next = next->next;
                gtidIntervalFree(next);
                return 1;
            } else {
                //B-E
                cur->gno_start = min(interval->gno_start, cur->gno_start);
                cur->gno_end = next->gno_end;
                cur->next = next->next;
                gtidIntervalFree(next);
                next = cur->next;
                changed = 1;
                continue;
            }
        } else if(next == NULL || interval->gno_end < next->gno_start - 1) {
            //C
            if(interval->gno_end < cur->gno_start - 1) {
                //ignore
                //C-A
                error_scope = "C-A";
                goto Error;
            } else if(interval->gno_end <= cur->gno_end + 1){
                //C-B
                error_scope = "C-B";
                goto Error;
            } else if(next == NULL || interval->gno_end < next->gno_start - 1) {
                //C-C
                gtidInterval* new_next = gtidIntervalDump(interval);
                new_next->next = cur->next;
                cur->next = new_next;
                return 1;
            } else if(interval->gno_end <= next->gno_end + 1){
                //C-D
                next->gno_start = cur->gno_start;
                return 1;
            } else {
                //C-E
                next->gno_start = cur->gno_start;
                changed = 1;
            }
        } else {
            //ignore
            // 1. gno_start <= gno_end   =>   B * A = 0(not exist)
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
    printf("\n code error [%s] %lld-%lld",error_scope, interval->gno_start, interval->gno_end);
    printf("cur %lld-%lld\n", cur->gno_start, cur->gno_end);
    if(next != NULL) {
        printf("next %lld-%lld\n", next->gno_start, next->gno_end);
    }
    exit(0);
}

int uuidSetAdd(uuidSet* uuid_set, rpl_gno gno)  {
    gtidInterval interval = {
        .gno_start = gno,
        .gno_end = gno,
        .next = NULL
    };
    return uuidSetAddGtidInterval(uuid_set, &interval);
}

void uuidSetRaise(uuidSet *uuid_set, rpl_gno watermark) {
    gtidInterval *cur = uuid_set->intervals;
    if (watermark < cur->gno_start - 1) {
        uuid_set->intervals = gtidIntervalNewRange(1, watermark);
        uuid_set->intervals->next = cur;
        return;
    }

    while (cur != NULL) {
        if (watermark > cur->gno_end + 1) {
            gtidInterval *temp = cur;
            cur = cur->next;
            gtid_free(temp);
            continue;
        }

        if (watermark == cur->gno_end + 1) {
            if (cur->next == NULL) {
                cur->gno_start = 1;
                cur->gno_end = watermark;
                uuid_set->intervals = cur;
                break;
            }
            if (watermark == cur->next->gno_start - 1) {
                gtidInterval *prev = cur;
                cur = cur->next;
                gtid_free(prev);
                cur->gno_start = 1;
                break;
            } else {
                cur->gno_end = watermark;
                cur->gno_start = 1;
                break;
            }
        }
        if (watermark < cur->gno_start - 1) {
            gtidInterval *temp = cur;
            cur = gtidIntervalNewRange(1, watermark);
            cur->next = temp;
            break;
        } else {
            cur->gno_start = 1;
            break;
        }
    }
    if (cur == NULL) {
        uuid_set->intervals = gtidIntervalNewRange(1, watermark);
    } else {
        uuid_set->intervals = cur;
    }
}

int uuidSetContains(uuidSet* uuid_set, rpl_gno gno) {
    gtidInterval *cur = uuid_set->intervals;
    while (cur != NULL) {
        if (gno >= cur->gno_start && gno <= cur->gno_end) {
            return 1;
        }
        cur = cur->next;
    }
    return 0;
}

rpl_gno uuidSetNext(uuidSet* uuid_set, int updateBeforeReturn) {
    if (uuid_set->intervals == NULL) {
        if (updateBeforeReturn) {
            uuid_set->intervals = gtidIntervalNew(1);
        }
        return 1;
    }

    rpl_gno next;
    if (uuid_set->intervals->gno_start != 1) {
        next = 1;
    } else {
        next = uuid_set->intervals->gno_end + 1;
    }
    if (updateBeforeReturn) {
        uuidSetAdd(uuid_set, next);
    }
    return next;
}

size_t uuidSetNextEncode(uuidSet* uuid_set, int updateBeforeReturn, char* buf) {
    size_t len = 0;
    len += writeBuf(buf + len, uuid_set->rpl_sid, strlen(uuid_set->rpl_sid));
    rpl_gno gno = uuidSetNext(uuid_set, updateBeforeReturn);
    len += writeBuf(buf + len, ":", 1);
    len += ll2string(buf + len, 21, gno);
    return len;
}

int uuidSetAppendUuidSet(uuidSet* uuid_set, uuidSet* other) {
    if(strcmp(uuid_set->rpl_sid, other->rpl_sid) != 0) {
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
            len +=  writeBuf(buf + len, ",", 1);
        }
    }
    return len;
}

uuidSet* gtidSetFindUuidSet(gtidSet* gtid_set, const char* rpl_sid, size_t len) {
    uuidSet *cur = gtid_set->uuid_sets;
    while(cur != NULL) {
        if (strncmp(cur->rpl_sid, rpl_sid, len) == 0) {
            break;
        }
        cur = cur->next;
    }
    return cur;
}

int gtidSetAdd(gtidSet* gtid_set, const char* rpl_sid, size_t rpl_sid_len ,rpl_gno gno) {
    uuidSet *cur = gtidSetFindUuidSet(gtid_set, rpl_sid, rpl_sid_len);
    if (cur == NULL) {
        cur = uuidSetNew(rpl_sid, rpl_sid_len, gno);
        gtidSetAppendUuidSet(gtid_set, cur);
        return 1;
    } else {
        return uuidSetAdd(cur, gno);
    }
}

char* uuidDecode(char* src, size_t src_len, long long* gno, int* rpl_sid_len) {
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
    *rpl_sid_len = index;
    return src;
}

void gtidSetRaise(gtidSet* gtid_set, const char* rpl_sid, size_t rpl_sid_len, rpl_gno watermark) {
    if (watermark == 0) return;
    uuidSet *cur = gtidSetFindUuidSet(gtid_set, rpl_sid, rpl_sid_len);
    if (cur == NULL) {
        cur = uuidSetNewRange(rpl_sid, rpl_sid_len, 1, watermark);
        gtidSetAppendUuidSet(gtid_set, cur);
    } else {
        uuidSetRaise(cur, watermark);
    }
}

void gtidSetAppendGtidSet(gtidSet* gtid_set, gtidSet* other) {
    uuidSet *cur = other->uuid_sets;
    while(cur != NULL) {
        uuidSet* src = gtidSetFindUuidSet(gtid_set, cur->rpl_sid, strlen(cur->rpl_sid));
        if(src != NULL) {
            uuidSetAppendUuidSet(src, cur);
        } else {
            gtidSetAppendUuidSet(gtid_set, uuidSetDump(cur));
        }
        cur = cur->next;
    }
}
