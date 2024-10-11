#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include "gtid.h"
#include "gtid_malloc.h"
#include "gtid_util.h"

/* 43 = 21(long long) + 1(-) + 21(long long) */
#define INTERVAL_ENCODE_MAX_LEN 43

gtidIntervalNode *gtidIntervalNodeNew(int level, gno_t start, gno_t end);
void gtidIntervalNodeFree(gtidIntervalNode* interval);
ssize_t gtidIntervalEncode(char *buf, size_t maxlen, gno_t start, gno_t end);
int gtidIntervalDecode(char* interval_str, size_t len, gno_t *pstart, gno_t *pend);
void uuidDup(char **pdup, size_t *pdup_len, const char* uuid, int uuid_len);
gno_t gtidSetAppend(gtidSet *gtid_set, uuidSet *uuid_set);

int test_gtidIntervalNew() {
    int level = 4, i;
    gtidIntervalNode* interval = gtidIntervalNodeNew(level,GTID_GNO_INITIAL,GTID_GNO_INITIAL);
    assert(interval->start == GTID_GNO_INITIAL);
    assert(interval->end == GTID_GNO_INITIAL);
    for (i = 0; i < level; i++) assert(interval->forwards[i] == NULL);
    gtidIntervalNodeFree(interval);

    interval = gtidIntervalNodeNew(level,-__LONG_LONG_MAX__,__LONG_LONG_MAX__);
    assert(interval->start == -__LONG_LONG_MAX__);
    assert(interval->end == __LONG_LONG_MAX__);
    for (i = 0; i < level; i++) assert(interval->forwards[i] == NULL);
    gtidIntervalNodeFree(interval);

    return 1;
}

int test_gtidIntervalDecode() {
    gno_t start, end;
    assert(gtidIntervalDecode("7", 1, &start, &end) == 0);
    assert(start == 7 && end == 7);

    assert(gtidIntervalDecode("1-9", 3, &start, &end) == 0);
    assert(start == 1 && end == 9);

    assert(gtidIntervalDecode("1-2-", 4, &start, &end) == 1);
    return 1;
}

int test_gtidIntervalEncode() {
    char buf[INTERVAL_ENCODE_MAX_LEN];
    size_t maxlen = INTERVAL_ENCODE_MAX_LEN, used_byte;
    gno_t start, end;

    used_byte = gtidIntervalEncode(buf, maxlen, 1, 10);
    assert(used_byte == 4);
    assert(memcmp(buf, "1-10", used_byte) == 0);

    used_byte = gtidIntervalEncode(buf, maxlen, 1, 1);
    assert(used_byte == 1);
    assert(memcmp(buf, "1", used_byte) == 0);

    assert(gtidIntervalDecode("712", 1, &start, &end) == 0);
    used_byte = gtidIntervalEncode(buf, maxlen, start, end);
    assert(strncmp(buf, "7", used_byte) == 0);
    return 1;
}

int test_uuidSetNew() {
    uuidSet* uuid_set = uuidSetNew("A", 1);
    assert(uuid_set->uuid_len == 1);
    assert(memcmp(uuid_set->uuid, "A\0", 2) == 0);
    uuidSetFree(uuid_set);

    uuid_set = uuidSetNew("A12345", 1);
    assert(uuid_set->uuid_len == 1);
    assert(memcmp(uuid_set->uuid, "A\0", 2) == 0);
    uuidSetFree(uuid_set);
    return 1;
}

int test_uuidSetDecode() {
    gtidIntervalNode *node;
    uuidSet* uuid_set;

    uuid_set = uuidSetDecode("A:1:3:5:7", 9);
    assert(uuid_set != NULL);
    assert(memcmp(uuid_set->uuid, "A\0", 2)== 0);

    node = uuid_set->intervals->header->forwards[0];
    assert(node->start == 1);
    assert(node->forwards[0]->start == 3);
    assert(node->forwards[0]->forwards[0]->start == 5);
    assert(node->forwards[0]->forwards[0]->forwards[0]->start == 7);
    assert(node->forwards[0]->forwards[0]->forwards[0]->forwards[0] == NULL);
    uuidSetFree(uuid_set);

    uuid_set = uuidSetDecode("A:1-6:8", 7);
    assert(memcmp(uuid_set->uuid, "A\0", 2) == 0);
    node = uuid_set->intervals->header->forwards[0];
    assert(node->start == 1);
    assert(node->end == 6);
    assert(node->forwards[0]->start == 8);
    assert(node->forwards[0]->end == 8);
    assert(node->forwards[0]->forwards[0] == NULL);
    uuidSetFree(uuid_set);

    uuid_set = uuidSetDecode("A:2-5:9adbsdada", 7);
    assert(memcmp(uuid_set->uuid, "A\0", 2) == 0);
    node = uuid_set->intervals->header->forwards[0];
    assert(node->start == 2);
    assert(node->end == 5);
    assert(node->forwards[0]->start == 9);
    assert(node->forwards[0]->end == 9);
    assert(node->forwards[0]->forwards[0] == NULL);
    uuidSetFree(uuid_set);

    uuid_set = uuidSetDecode("A",1);
    assert(uuid_set && uuidSetCount(uuid_set) == 0);
    uuidSetFree(uuid_set);

    return 1;
}

int test_uuidSetDup() {
    uuidSet* uuid_set = uuidSetNew("A", 1), *dup;

    dup = uuidSetDup(uuid_set);
    assert(uuidSetCount(dup) == 0 && uuidSetNext(dup,0) == 1);
    assert(!uuidSetContains(dup,10));
    uuidSetFree(dup);

    for (int i = 1; i < 128; i++) {
        uuidSetAdd(uuid_set,i*10,i*10+4);

        dup = uuidSetDup(uuid_set);

        assert(uuidSetCount(dup) == i*5 && uuidSetNext(dup,0) == i*10+5);

        assert(!uuidSetContains(dup,1));

        for (int j = 1; j < i; j++) {
            assert(!uuidSetContains(dup,j*10-1));
            assert(uuidSetContains(dup,j*10));
            assert(!uuidSetContains(dup,j*10+5));
        }

        uuidSetFree(dup);
    }

    assert(uuid_set->intervals->level > 1);
    uuidSetFree(uuid_set);
    return 1;
}

int test_uuidSetEstimatedEncodeBufferSize() {
    uuidSet* uuid_set = uuidSetNew("A", 1);
    uuidSetAdd(uuid_set, 1, 1);
    assert(uuidSetEstimatedEncodeBufferSize(uuid_set) > 3);
    uuidSetAdd(uuid_set, 3, 3);
    assert(uuidSetEstimatedEncodeBufferSize(uuid_set) > 5);
    uuidSetAdd(uuid_set, 5, 5);
    assert(uuidSetEstimatedEncodeBufferSize(uuid_set) > 7);
    uuidSetFree(uuid_set);

    char* decode_str = "A:1:3:5:7:9";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    size_t max_len = uuidSetEstimatedEncodeBufferSize(uuid_set);
    assert(max_len > strlen(decode_str));
    uuidSetFree(uuid_set);
    return 1;
}

int test_uuidSetEncode() {
    char *decode_str = "A:1:2:3:4:5";
    uuidSet* uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    size_t max_len = uuidSetEstimatedEncodeBufferSize(uuid_set);
    char uuid_set_str[max_len];
    size_t len = uuidSetEncode(uuid_set_str, max_len, uuid_set);
    uuid_set_str[len] = '\0';
    assert(strcmp(uuid_set_str, "A:1-5") == 0);
    uuidSetFree(uuid_set);
    return 1;
}

int test_uuidSetInvalidArg() {
    uuidSet *uuid_set;
    assert(uuidSetDecode("",0) == NULL);
    uuid_set = uuidSetDecode("foobar",6);
    assert(uuidSetCount(uuid_set) == 0);
    uuidSetFree(uuid_set);

    uuid_set = uuidSetDecode("A:2-1",5);
    assert(uuid_set == NULL);

    uuid_set = uuidSetDecode("A:foobar",8);
    assert(uuid_set == NULL);

    uuid_set = uuidSetDecode("A:3-10",6);
    assert(uuidSetContains(uuid_set,0) == 0);
    assert(uuidSetAdd(uuid_set,0,0) == 0);
    assert(uuidSetAdd(uuid_set,2,1) == 0);
    uuidSetFree(uuid_set);

    return 1;
}

int test_uuidSetAddInterval() {
    size_t maxlen = 100;
    char* decode_str;
    char buf[maxlen];
    size_t buf_len;
    uuidSet* uuid_set;

    //() + (1-2) = (1-2)
    decode_str = "A";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 1, 2) == 2);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:1-2"));
    assert(strncmp(buf, "A:1-2", buf_len) == 0);
    uuidSetFree(uuid_set);

    //  A  B  C
    // in A
    //(4-5,7-8,10-11) + (1-2) = (1-2,4,-5,7-8,10-11)
    decode_str = "A:4-5:7-8:10-11";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 1, 2) == 2);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:1-2:4-5:7-8:10-11"));
    assert(strncmp(buf, "A:1-2:4-5:7-8:10-11", buf_len) == 0);
    uuidSetFree(uuid_set);

    //  A {start} B {end} C
    // in B
    //(1-5:7-8:10-11) + (2-3) = (1-5:7-8:10-11)
    decode_str = "A:1-5:7-8:10-11";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 2, 3) == 0);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:1-5:7-8:10-11"));
    assert(strncmp(buf, "A:1-5:7-8:10-11", buf_len) == 0);
    uuidSetFree(uuid_set);

    //  A {start} B {end} C
    // in C
    // (1-2:7-8:10-11) + (4-5) = (1-2:4-5:7-8:10-11)
    decode_str = "A:1-2:7-8:10-11";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 4, 5) == 2);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:1-2:4-5:7-8:10-11"));
    assert(strncmp(buf, "A:1-2:4-5:7-8:10-11", buf_len) == 0);
    uuidSetFree(uuid_set);

    //  A {start} B {end} C
    // in A + B
    decode_str = "A:3-5:7-8:10-11";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 1, 4) == 2);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:1-5:7-8:10-11"));
    assert(strncmp(buf, "A:1-5:7-8:10-11", buf_len) == 0);
    uuidSetFree(uuid_set);

    //  A {start} B {end} C
    // in B + C
    decode_str = "A:1-3:7-8:10-11";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 2, 5) == 2);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:1-5:7-8:10-11"));
    assert(strncmp(buf, "A:1-5:7-8:10-11", buf_len) == 0);
    uuidSetFree(uuid_set);

    //  A {start} B {end} C
    // in A + C
    decode_str = "A:2-3:7-8:10-11";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 1, 5) == 3);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:1-5:7-8:10-11"));
    assert(strncmp(buf, "A:1-5:7-8:10-11", buf_len) == 0);
    uuidSetFree(uuid_set);

    // A {start} B {end} C {all_next_start} D {all_next_end} E
    // A + D
    //(A:2-3:6-8:11-13) + (A:1-7) = (A:1-8:11-13)
    decode_str = "A:2-3:6-8:11-13";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 1, 7) == 3);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:1-8:11-13"));
    assert(strncmp(buf, "A:1-8:11-13", buf_len) == 0);
    uuidSetFree(uuid_set);

    //(A:2-3:6-8:11-13:15-20) + (A:1-11) = (A:1-13:15-20)
    decode_str = "A:2-3:6-8:11-13";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 1, 7) == 3);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:1-8:11-13"));
    assert(strncmp(buf, "A:1-8:11-13", buf_len) == 0);
    uuidSetFree(uuid_set);

    //(A:2-3:6-8:11-13) + (A:1-12) = (A:1-13)
    decode_str = "A:2-3:6-8:11-13";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 1, 12) == 5);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:1-13"));
    assert(strncmp(buf, "A:1-13", buf_len) == 0);
    uuidSetFree(uuid_set);

    // B + D
    //(3-5:7-8:10-11) + (1-4) = (1-5:7-8:10-11)
    decode_str = "A:3-5:7-9:11-12";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 4, 8) == 1);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:3-9:11-12"));
    assert(strncmp(buf, "A:3-9:11-12", buf_len) == 0);
    uuidSetFree(uuid_set);

    //(3-5:7-9:11-12) + (A:4-11) = (A:3-12)
    decode_str = "A:3-5:7-9:11-12";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 4, 11) == 2);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:3-12"));
    assert(strncmp(buf, "A:3-12", buf_len) == 0);
    uuidSetFree(uuid_set);

    //C + D
    //(A:2-3:7-9:11-12) + (A:5-8)=(A:2-3:5-9:11-12)
    decode_str = "A:2-3:7-9:11-12";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 5, 8) == 2);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:2-3:5-9:11-12"));
    assert(strncmp(buf, "A:2-3:5-9:11-12", buf_len) == 0);
    uuidSetFree(uuid_set);

    //(A:2-3:7-9:11-12:15-16) + (A:5-11)=(A:2-3:5-12:15-16)
    decode_str = "A:2-3:7-9:11-12:15-16";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 5, 11) == 3);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:2-3:5-12:15-16"));
    assert(strncmp(buf, "A:2-3:5-12:15-16", buf_len) == 0);
    uuidSetFree(uuid_set);

    //D
    decode_str = "A:2-3:7-9:11-12";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 5, 8) == 2);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:2-3:5-9:11-12"));
    assert(strncmp(buf, "A:2-3:5-9:11-12", buf_len) == 0);
    uuidSetFree(uuid_set);

    decode_str = "A:2-3:7-9:11-19";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 13, 16) == 0);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:2-3:7-9:11-19"));
    assert(strncmp(buf, "A:2-3:7-9:11-19", buf_len) == 0);
    uuidSetFree(uuid_set);

    //A + E
    decode_str = "A:2-3:7-9:11-12";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 1, 14) == 7);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:1-14"));
    assert(strncmp(buf, "A:1-14", buf_len) == 0);
    uuidSetFree(uuid_set);


    decode_str = "A:2-3:7-9:12-13:15-19";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 1, 13) == 6);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:1-13:15-19"));
    assert(strncmp(buf, "A:1-13:15-19", buf_len) == 0);
    uuidSetFree(uuid_set);

    //B+E
    //(A:2-4:7-9:12-13:15-19) + (A:3-20) = (A:2-20)
    decode_str = "A:2-4:7-9:12-13:15-19";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 3, 20) == 6);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:2-20"));
    assert(strncmp(buf, "A:2-20", buf_len) == 0);
    uuidSetFree(uuid_set);


    //(A:2-4:7-9:11-13:15-19) + (A:3-12) = (A:2-13:15-19)
    decode_str = "A:2-4:7-9:11-13:15-19";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 3, 12) == 3);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:2-13:15-19"));
    assert(strncmp(buf, "A:2-13:15-19", buf_len) == 0);
    uuidSetFree(uuid_set);

    //C+E
    //(A:2-4:7-9:11-13:15-19) + (A:6-20) = (A:2-4:6-20)
    decode_str = "A:2-4:7-9:11-13:15-19";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 6, 20) == 4);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:2-4:6-20"));
    assert(strncmp(buf, "A:2-4:6-20", buf_len) == 0);
    uuidSetFree(uuid_set);

    //(A:2-4:7-9:11-13:16-19) + (A:6-14) = (A:2-4:6-14:16-19)
    decode_str = "A:2-4:7-9:11-13:16-19";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 6, 14) == 3);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:2-4:6-14:16-19"));
    assert(strncmp(buf, "A:2-4:6-14:16-19", buf_len) == 0);
    uuidSetFree(uuid_set);

    //D+E
    //(A:2-4:7-9:11-13:15-19) + (A:8-20) = (A:2-4:7-20)
    decode_str = "A:2-4:7-9:11-13:15-19";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 8, 20) == 3);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:2-4:7-20"));
    assert(strncmp(buf, "A:2-4:7-20", buf_len) == 0);
    uuidSetFree(uuid_set);

    //(A:2-4:7-9:11-13:15-19) + (A:8-12) = (A:2-4:7-13:15-19)
    decode_str = "A:2-4:7-9:11-13:15-19";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 8, 12) == 1);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:2-4:7-13:15-19"));
    assert(strncmp(buf, "A:2-4:7-13:15-19", buf_len) == 0);
    uuidSetFree(uuid_set);

    //E
    decode_str = "A:2-3:7-9:11-12";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 14, 20) == 7);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:2-3:7-9:11-12:14-20"));
    assert(strncmp(buf, "A:2-3:7-9:11-12:14-20", buf_len) == 0);
    uuidSetFree(uuid_set);

    //(A:2-3:7-8:11-12) + (A:10-13) = (A:2-3:7-8:10-13)
    decode_str = "A:2-3:7-8:11-12";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 10, 13) == 2);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:2-3:7-8:10-13"));
    assert(strncmp(buf, "A:2-3:7-8:10-13", buf_len) == 0);
    uuidSetFree(uuid_set);

    decode_str = "A:1:3";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 2, 2) == 1);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:1-3"));
    assert(strncmp(buf, "A:1-3", buf_len) == 0);
    uuidSetFree(uuid_set);

    decode_str = "A:1:4";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 2, 3) == 2);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:1-4"));
    assert(strncmp(buf, "A:1-4", buf_len) == 0);
    uuidSetFree(uuid_set);

    decode_str = "A:1:4";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 2, 3) == 2);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:1-4"));
    assert(strncmp(buf, "A:1-4", buf_len) == 0);
    uuidSetFree(uuid_set);

    decode_str = "A:4-5";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 2, 3) == 2);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:2-5"));
    assert(strncmp(buf, "A:2-5", buf_len) == 0);
    uuidSetFree(uuid_set);

    decode_str = "A:4-5";
    uuid_set = uuidSetDecode(decode_str, strlen(decode_str));
    assert(uuidSetAdd(uuid_set, 6, 7) == 2);
    buf_len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(buf_len == strlen("A:4-7"));
    assert(strncmp(buf, "A:4-7", buf_len) == 0);
    uuidSetFree(uuid_set);

    return 1;
}

int test_uuidSetAdd() {
    size_t maxlen = 100, len;
    char buf[maxlen];
    uuidSet* uuid_set;
    gtidIntervalNode *node;

    uuid_set = uuidSetNew("A",1);
    uuidSetAdd(uuid_set, 1, 1);
    uuidSetAdd(uuid_set, 3, 3);
    node = uuid_set->intervals->header->forwards[0]->forwards[0];
    assert(node->start == 3);
    assert(node->end == 3);
    len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(len == 5); //A:1:3
    assert(strncmp(buf, "A:1:3", 5) == 0);
    uuidSetFree(uuid_set);

    uuid_set = uuidSetNew("A", 1);
    uuidSetAdd(uuid_set, 5, 5);
    uuidSetAdd(uuid_set, 6, 6);
    uuidSetAdd(uuid_set, 8, 8);
    uuidSetAdd(uuid_set, 9, 9);
    assert(uuid_set->intervals->node_count == 3);
    //add 9 to 5-6,8-9
    assert(uuidSetAdd(uuid_set, 9, 9) == 0);
    uuidSetFree(uuid_set);

    uuid_set = uuidSetNew("A", 1);
    uuidSetAdd(uuid_set, 1 , 1 );
    uuidSetAdd(uuid_set, 5 , 5 );
    uuidSetAdd(uuid_set, 6 , 6 );
    uuidSetAdd(uuid_set, 11, 11);
    uuidSetAdd(uuid_set, 13, 13);
    uuidSetAdd(uuid_set, 20, 20);
    uuidSetAdd(uuid_set, 19, 19);
    uuidSetAdd(uuid_set, 1 , 1 );
    uuidSetAdd(uuid_set, 12, 12);
    uuidSetAdd(uuid_set, 3 , 3 );
    uuidSetAdd(uuid_set, 13, 13);
    uuidSetAdd(uuid_set, 13, 13);
    uuidSetAdd(uuid_set, 14, 14);
    uuidSetAdd(uuid_set, 12, 12);
    //Manual created case: result should be A:1:3:5-6:11-14:19-20
    node = uuid_set->intervals->header->forwards[0];
    assert(node->start == 1 );
    assert(node->end == 1);
    assert(node->forwards[0]->start == 3);
    assert(node->forwards[0]->end == 3);
    assert(node->forwards[0]->forwards[0]->start == 5);
    assert(node->forwards[0]->forwards[0]->end == 6);
    assert(node->forwards[0]->forwards[0]->forwards[0]->start == 11);
    assert(node->forwards[0]->forwards[0]->forwards[0]->end == 14);
    assert(node->forwards[0]->forwards[0]->forwards[0]->forwards[0]->start == 19);
    assert(node->forwards[0]->forwards[0]->forwards[0]->forwards[0]->end == 20);
    assert(memcmp(uuid_set->uuid, "A\0", 2) == 0);
    uuidSetFree(uuid_set);

    uuid_set = uuidSetNew("A", 1);
    uuidSetAdd(uuid_set, 9, 9);
    uuidSetAdd(uuid_set, 8, 8);
    //Add 8 to 9
    node = uuid_set->intervals->header->forwards[0];
    assert(node->start == 8);
    assert(node->end == 9);
    assert(memcmp(uuid_set->uuid, "A\0", 2) == 0);

    uuidSetAdd(uuid_set, 6, 6);
    //Add 6 to 8-9
    node = uuid_set->intervals->header->forwards[0];
    assert(node->start == 6);
    assert(node->end == 6);
    assert(node->forwards[0]->start == 8);
    assert(node->forwards[0]->end == 9);
    assert(memcmp(uuid_set->uuid, "A\0", 2) == 0);

    //"Add 8 to 8-9"
    assert(uuidSetAdd(uuid_set, 8, 8) == 0);

    uuidSetAdd(uuid_set, 7, 7);
    //"Add 7 to 6,8-9"
    node = uuid_set->intervals->header->forwards[0];
    assert(node->start == 6);
    assert(node->end == 9);
    assert(node->forwards[0] == NULL);
    assert(memcmp(uuid_set->uuid, "A\0", 2) == 0);

    uuidSetAdd(uuid_set, 100, 100);
    //"Add 100 to 6-9"
    node = uuid_set->intervals->header->forwards[0];
    assert(node->start == 6);
    assert(node->end == 9);
    assert(node->forwards[0]->start == 100);
    assert(node->forwards[0]->end == 100);
    assert(memcmp(uuid_set->uuid, "A\0", 2) == 0);

    uuidSetFree(uuid_set);


    uuid_set = uuidSetNew("ABC",1);
    uuidSetAdd(uuid_set, 9, 9);
    //Create an new uuid set with 9
    node = uuid_set->intervals->header->forwards[0];
    assert(node->start == 9);
    assert(node->end == 9);
    assert(memcmp(uuid_set->uuid, "A\0", 2) == 0);

    uuidSetAdd(uuid_set, 7, 7);
    //"Add 7 to 9"
    node = uuid_set->intervals->header->forwards[0];
    assert(node->start == 7);
    assert(node->end == 7);
    assert(node->forwards[0]->start == 9);
    assert(node->forwards[0]->end == 9);
    assert(memcmp(uuid_set->uuid, "A\0", 2) == 0);

    uuidSetFree(uuid_set);
    return 1;
}

int test_uuidSetAddChaos() {
    int mask = 20;
    int range = 1<<mask, round = 1024;
    uuidSet *uuid_set = uuidSetNew("A",1);
    srand(time(NULL));
    for (int i = 0; i < round; i++) {
        while (uuid_set->intervals->gno_count < range) {
            int gno, start, end, tmp;

            gno = rand()%range + 1;
            uuidSetAdd(uuid_set, gno, gno);

            start = rand()%range + 1, end = start + rand() % mask;
            if (start > end) {
                tmp = start;
                start = end;
                end = tmp;
            }
            uuidSetAdd(uuid_set, start, end);
        }
    }
    uuidSetFree(uuid_set);
    return 1;
}

int test_uuidSetRemove() {
    uuidSet *uuid_set = uuidSetNew("A",1);
    assert(uuidSetRemove(uuid_set,0,1) == 0);
    assert(uuidSetRemove(uuid_set,1,1) == 0);
    assert(uuidSetCount(uuid_set) == 0 && uuidSetNext(uuid_set,0) == 1);

    uuidSetAdd(uuid_set,10,10);
    assert(uuidSetCount(uuid_set) == 1 && uuidSetNext(uuid_set,0) == 11);
    assert(uuidSetRemove(uuid_set,3,5) == 0);
    assert(uuidSetCount(uuid_set) == 1 && uuidSetNext(uuid_set,0) == 11);
    assert(uuidSetRemove(uuid_set,11,12) == 0);
    assert(uuidSetCount(uuid_set) == 1 && uuidSetNext(uuid_set,0) == 11);
    assert(uuidSetRemove(uuid_set,1,10) == 1);
    assert(uuidSetCount(uuid_set) == 0 && uuidSetNext(uuid_set,0) == 1);

    uuidSetAdd(uuid_set,10,11);
    assert(uuidSetCount(uuid_set) == 2 && uuidSetNext(uuid_set,0) == 12);
    assert(uuidSetRemove(uuid_set,1,20) == 2);
    assert(uuidSetCount(uuid_set) == 0 && uuidSetNext(uuid_set,0) == 1);

    uuidSetAdd(uuid_set,10,11);
    assert(uuidSetCount(uuid_set) == 2 && uuidSetNext(uuid_set,0) == 12);
    assert(uuidSetRemove(uuid_set,1,10) == 1);
    assert(uuidSetCount(uuid_set) == 1 && uuidSetNext(uuid_set,0) == 12);
    assert(!uuidSetContains(uuid_set,10));
    assert(uuidSetContains(uuid_set,11));
    assert(uuidSetRemove(uuid_set,11,20) == 1);
    assert(uuidSetCount(uuid_set) == 0 && uuidSetNext(uuid_set,0) == 1);
    assert(!uuidSetContains(uuid_set,11));

    uuidSetAdd(uuid_set,10,15);
    assert(uuidSetRemove(uuid_set,10,20));
    assert(uuidSetCount(uuid_set) == 0 && uuidSetNext(uuid_set,0) == 1);
    assert(!uuidSetContains(uuid_set,10));

    uuidSetAdd(uuid_set,10,15);
    uuidSetAdd(uuid_set,20,25);
    assert(uuidSetRemove(uuid_set,13,25) == 9);
    assert(uuidSetCount(uuid_set) == 3 && uuidSetNext(uuid_set,0) == 13);
    assert(uuidSetContains(uuid_set,12));
    assert(!uuidSetContains(uuid_set,25));

    uuidSetAdd(uuid_set,10,15);
    uuidSetAdd(uuid_set,20,25);
    uuidSetAdd(uuid_set,30,35);
    assert(uuidSetCount(uuid_set) == 18 && uuidSetNext(uuid_set,0) == 36);
    assert(uuidSetRemove(uuid_set,13,33) == 13);
    assert(uuidSetCount(uuid_set) == 5 && uuidSetNext(uuid_set,0) == 36);
    assert(uuidSetContains(uuid_set,12));
    assert(!uuidSetContains(uuid_set,13));
    assert(!uuidSetContains(uuid_set,20));
    assert(!uuidSetContains(uuid_set,30));
    assert(uuidSetContains(uuid_set,34));

    uuidSetFree(uuid_set);
    return 1;
}

int test_uuidSetRemoveChaos() {
    /* size_t S = 4, E = 16, s = 2, e = 8, xlen = e-s, ROUND = 32; */
    /* size_t S = 8, E = 32, s = 4, e = 16, xlen = e-s, ROUND = 32; */
    size_t S = 512, E = 2048, s = 256, e = 1024, xlen = e-s, ROUND = 64;
    assert(s < S && S < e && e < E && (xlen%2) == 0);
    gno_t *array = gtid_malloc(sizeof(gno_t)*xlen);
    uuidSet *uuid_set = uuidSetNew("A",1);

    uuidSetAdd(uuid_set,S,E);

    for (size_t i = 0; i < xlen; i++) array[i] = s+i;

    for (size_t round = 0; round < ROUND; round++) {
        srand(round);
        for (size_t i = 0; i < xlen; i++) {
            size_t chosen = rand()%(xlen-i);
            gno_t tmp = array[xlen-i-1];
            array[xlen-i-1] = array[chosen];
            array[chosen] = tmp;
        }

        for (size_t i = 0; i < xlen; i+=2) {
            gno_t l = array[i], r = array[i+1];
            if (l > r) {
                gno_t tmp = l;
                l = r;
                r = tmp;
            }
            uuidSetRemove(uuid_set,l,r);
        }

        assert(uuidSetCount(uuid_set) == E-e+1 && uuidSetNext(uuid_set,0) == E+1);
        for (size_t i = s; i < e; i++) {
            if (i <= e)
                assert(!uuidSetContains(uuid_set,i));
            else
                assert(uuidSetContains(uuid_set,i));
        }
    }

    uuidSetFree(uuid_set);
    gtid_free(array);
    return 1;
}

int test_uuidSetDiff() {
    uuidSet *uuid_set = uuidSetNew("A",1), *src = uuidSetNew("A",1);

    uuidSetAdd(uuid_set,10,10);
    assert(uuidSetContains(uuid_set,10));

    uuidSetDiff(uuid_set,src);
    assert(uuidSetContains(uuid_set,10));

    uuidSetAdd(src,8,12);
    uuidSetDiff(uuid_set,src);
    assert(uuidSetCount(uuid_set) == 0 && !uuidSetContains(uuid_set,10));

    uuidSetAdd(uuid_set,10,10);
    uuidSetAdd(uuid_set,20,20);
    uuidSetDiff(uuid_set,uuid_set);
    assert(uuidSetCount(uuid_set) == 0 && !uuidSetContains(uuid_set,10) && !uuidSetContains(uuid_set,20));

    uuidSetFree(uuid_set);
    uuidSetFree(src);
    return 1;
}

int test_uuidSetContains() {
    gtidIntervalNode *node;
    uuidSet* uuid_set = uuidSetNew("A", 1);
    uuidSetAdd(uuid_set, 1, 5);
    assert(uuidSetContains(uuid_set, 1) == 1);
    assert(uuidSetContains(uuid_set, 6) == 0);
    uuidSetFree(uuid_set);

    uuid_set = uuidSetNew("A",1);
    uuidSetAdd(uuid_set, 5, 5);
    uuidSetAdd(uuid_set, 8, 8);
    uuidSetAdd(uuid_set, 1, 6);
    node = uuid_set->intervals->header->forwards[0];
    assert(node->start == 1);
    assert(node->end == 6);
    assert(node->forwards[0]->start == 8);
    assert(node->forwards[0]->end == 8);
    assert(node->forwards[0]->forwards[0] == NULL);
    assert(memcmp(uuid_set->uuid, "A\0", 2) == 0);

    //A:1-6:8
    //1 is in A:1-8
    assert(uuidSetContains(uuid_set, 1) == 1);
    //3 is in A:1-8
    assert(uuidSetContains(uuid_set, 3) == 1);
    //6 is in A:1-8
    assert(uuidSetContains(uuid_set, 6) == 1);
    //7 is not in A:1-8
    assert(uuidSetContains(uuid_set, 7) == 0);
    //8 is in A:1-8
    assert(uuidSetContains(uuid_set, 8) == 1);
    //30 is not in A:1-8
    assert(uuidSetContains(uuid_set, 30) == 0);

    uuidSetFree(uuid_set);

    return 1;
}

int test_uuidSetNext() {
    size_t maxlen = 100;
    char uuidset_str[maxlen];
    ssize_t uuidset_len;

    uuidSet* uuid_set = uuidSetNew("A", 1);
    assert(1 == uuidSetNext(uuid_set, 1));
    assert(2 == uuidSetNext(uuid_set, 0));

    uuidSetAdd(uuid_set, 3, 4);
    assert(5 == uuidSetNext(uuid_set, 0));
    assert(5 == uuidSetNext(uuid_set, 1));

    uuidSetAdd(uuid_set, 10, 11);
    assert(uuidSetNext(uuid_set, 1) == 12);

    uuidSetAdd(uuid_set, 5, 9);
    assert(uuidSetNext(uuid_set, 0) == 13);

    uuidset_len = uuidSetEncode(uuidset_str, maxlen, uuid_set);
    uuidset_str[uuidset_len] = '\0';
    assert(strcmp(uuidset_str, "A:1:3-12") == 0);

    uuidSetFree(uuid_set);
    return 1;
}

ssize_t uuidSetNextEncode(char *buf, size_t maxlen, uuidSet* uuid_set, int update) {
    gno_t gno = uuidSetNext(uuid_set, update);
    return uuidGnoEncode(buf, maxlen, uuid_set->uuid, uuid_set->uuid_len, gno);
}

int test_uuidSetNextEncode() {
    uuidSet* uuid_set = uuidSetNew("A", 1);
    uuidSetAdd(uuid_set, 1, 5);
    size_t maxlen = 100, len, next_len;
    char buf[maxlen], next[maxlen];

    len = uuidSetNextEncode(buf, maxlen, uuid_set, 1);
    assert(3 == len);
    assert(strncmp(buf, "A:6", len) == 0);
    uuidSetAdd(uuid_set, 1, 8);
    len = uuidSetNextEncode(buf, maxlen, uuid_set, 1);
    assert(3 == len);
    assert(strncmp(buf, "A:9", len) == 0);
    uuidSetFree(uuid_set);

    char *gtidset = "A:1-7,B:9:11-13:20";
    gtidSet* gtid_set = gtidSetDecode(gtidset, strlen(gtidset));
    uuidSet* B = gtidSetFind(gtid_set, "B", 1);
    next_len = uuidSetNextEncode(next, maxlen, B, 1);
    assert(strncmp("B:21", next, next_len) == 0);
    assert(uuidSetContains(B, 21));

    next_len = uuidSetNextEncode(next, maxlen, B, 1);
    next[next_len] = '\0';
    assert(strcmp("B:22", next) == 0);
    assert(uuidSetContains(B, 22));

    next_len = uuidSetNextEncode(next, maxlen, B, 1);
    next_len = uuidSetNextEncode(next, maxlen, B, 1);
    next_len = uuidSetNextEncode(next, maxlen, B, 1);
    next_len = uuidSetNextEncode(next, maxlen, B, 1);
    next_len = uuidSetNextEncode(next, maxlen, B, 1);
    next[next_len] = '\0';
    long long gno = 0;
    int sid_len = 0;
    uuidGnoDecode(next, next_len, &gno, &sid_len);
    assert(strcmp("B:27", next) == 0);
    assert(gno == 27);
    assert(uuidSetContains(B, gno));

    next_len = uuidSetNextEncode(next, maxlen, B, 1);
    assert(strcmp("B:28", next) == 0);
    assert(uuidSetContains(B, gno));

    next_len = uuidSetNextEncode(next, maxlen, B, 1);
    assert(strcmp("B:29", next) == 0);
    assert(uuidSetContains(B, gno));

    next_len = uuidSetNextEncode(next, maxlen, B, 1);
    assert(strcmp("B:30", next) == 0);
    assert(uuidSetContains(B, 26));
    gtidSetFree(gtid_set);
    return 1;
}

#define GTID_INTERVAL_MEMORY (sizeof(gtidIntervalNode) + sizeof(gtidIntervalNode*))

int test_gtidSetNew() {
    gtidSet* gtid_set = gtidSetNew();
    assert(gtid_set->header == NULL);
    assert(gtid_set->tail == NULL);
    gtidSetFree(gtid_set);
    return 1;
}

int test_gtidSetDup() {
    char buf[128];
    size_t len;
    gtidSet* gtid_set = gtidSetNew(), *dup;

    dup = gtidSetDup(gtid_set);
    assert(dup->header == NULL && dup->tail == NULL);
    gtidSetFree(dup);

    gtidSetAdd(gtid_set,"A",1,10,10);
    dup = gtidSetDup(gtid_set);
    len = gtidSetEncode(buf,sizeof(buf),dup);
    assert(!strncmp(buf,"A:10",len));
    gtidSetFree(dup);

    gtidSetAdd(gtid_set,"B",1,10,10);
    dup = gtidSetDup(gtid_set);
    len = gtidSetEncode(buf,sizeof(buf),dup);
    assert(!strncmp(buf,"A:10,B:10",len));
    gtidSetFree(dup);

    gtidSetAdd(gtid_set,"A",1,20,20);
    gtidSetAdd(gtid_set,"B",1,20,20);
    dup = gtidSetDup(gtid_set);
    len = gtidSetEncode(buf,sizeof(buf),dup);
    assert(!strncmp(buf,"A:10:20,B:10:20",len));
    gtidSetFree(dup);

    gtidSetFree(gtid_set);
    return 1;
}

int test_gtidSetDecode() {
    char* gtid_set_str = "A:1,B:1";
    gtidSet* gtid_set = gtidSetDecode(gtid_set_str, 7);
    assert(strcmp(gtid_set->header->uuid, "A") == 0);
    assert(gtid_set->header->intervals->header->forwards[0]->start == 1);
    assert(gtid_set->header->intervals->header->forwards[0]->end == 1);
    assert(strcmp(gtid_set->header->next->uuid, "B") == 0);
    assert(gtid_set->header->next->intervals->header->forwards[0]->start == 1);
    assert(gtid_set->header->next->intervals->header->forwards[0]->end == 1);
    gtidSetFree(gtid_set);

    gtid_set_str = "A:1-7,B:9:11-13:20ABC";
    gtid_set = gtidSetDecode(gtid_set_str, strlen(gtid_set_str)-3);
    size_t maxlen = 100;
    char gtid_str[maxlen];
    int len = gtidSetEncode(gtid_str, maxlen, gtid_set);
    assert(len == strlen(gtid_set_str) - 3);
    assert(memcmp(gtid_set_str,gtid_str,len) == 0);
    gtidSetFree(gtid_set);

    gtid_set_str = "foo bar";
    gtid_set = gtidSetDecode(gtid_set_str, strlen(gtid_set_str));
    assert(gtidSetCount(gtid_set) == 0);
    gtidSetFree(gtid_set);

    gtid_set_str = "";
    gtid_set = gtidSetDecode(gtid_set_str, strlen(gtid_set_str));
    assert(gtid_set && gtidSetCount(gtid_set) == 0);
    gtidSetFree(gtid_set);

    uuidSet *uuid_set;
    gtid_set_str = "A:,B:2,C:";
    gtid_set = gtidSetDecode(gtid_set_str, strlen(gtid_set_str));
    assert(gtid_set);
    uuid_set = gtidSetFind(gtid_set,"A",1);
    assert(uuid_set && uuidSetCount(uuid_set) == 0);
    uuid_set = gtidSetFind(gtid_set,"B",1);
    assert(uuid_set && uuidSetCount(uuid_set) == 1);
    uuid_set = gtidSetFind(gtid_set,"C",1);
    assert(uuid_set && uuidSetCount(uuid_set) == 0);
    gtidSetFree(gtid_set);

    return 1;
}

int test_gtidSetEstimatedEncodeBufferSize() {

    //about max
    char gtid_set_str[2000] = "";
    for(int i = 0; i < 1000; i++) {
        gtid_set_str[i] = 'A';
    }
    gtid_set_str[1000] = ':';
    size_t len = ll2string(gtid_set_str + 1001, 1000, __LONG_LONG_MAX__ );
    gtid_set_str[1001 + len] = '-';
    len += ll2string(gtid_set_str + 1002 + len, 1000, __LONG_LONG_MAX__ );
    gtid_set_str[1002 + len] = '\0';
    gtidSet* gtid_set = gtidSetDecode(gtid_set_str, strlen(gtid_set_str));
    size_t max_len = gtidSetEstimatedEncodeBufferSize(gtid_set);
    assert(max_len > strlen(gtid_set_str));
    gtidSetFree(gtid_set);

    //about empty
    gtid_set = gtidSetNew();
    max_len = gtidSetEstimatedEncodeBufferSize(gtid_set);
    //"estimated empty gtid set to string len > 0"
    assert(max_len > 0);
    gtidSetFree(gtid_set);

    return 1;
}

int test_gtidSetEncode() {
    gtidSet* gtid_set = gtidSetNew();
    size_t maxlen = 100;
    char gtid_str[maxlen];
    size_t len = gtidSetEncode(gtid_str, maxlen, gtid_set);
    //empty encode len == 0
    assert(len == 0);

    gtidSetAdd(gtid_set, "A", 1, 1, 1);
    len = gtidSetEncode(gtid_str, maxlen, gtid_set);
    assert(len == 3);
    assert(strncmp(gtid_str, "A:1", len) == 0);
    gtidSetAdd(gtid_set, "B", 1, 1, 1);
    len = gtidSetEncode(gtid_str, maxlen, gtid_set);
    assert(len == 7);
    assert(strncmp(gtid_str, "A:1,B:1", len) == 0);
    gtidSetFree(gtid_set);


    char* gtid_set_str = "A:1-7,B:9:11-13:20";
    gtid_set = gtidSetDecode(gtid_set_str, strlen(gtid_set_str));
    len = gtidSetEncode(gtid_str, maxlen, gtid_set);
    //encode & decode A:1-7,B:9:11-13:20 string len equal
    assert(len == strlen(gtid_set_str));
    //encode & decode A:1-7,B:9:11-13:20
    assert(strncmp(gtid_str, gtid_set_str, len) == 0);

    gtidSetFree(gtid_set);
    return 1;
}

int test_gtidSetFind() {
    gtidSet* gtid_set = gtidSetDecode("A:1,B:2", 7);
    uuidSet* A = gtidSetFind(gtid_set, "A", 1);
    assert(A != NULL);
    assert(strcmp(A->uuid, "A") == 0);
    assert(A->intervals->header->forwards[0]->start == 1);
    uuidSet* B = gtidSetFind(gtid_set, "B", 1);
    assert(B != NULL);
    assert(strcmp(B->uuid, "B") == 0);
    assert(B->intervals->header->forwards[0]->start == 2);
    gtidSetFree(gtid_set);
    return 1;
}

int test_gtidSetAdd() {
    gtidSet* gtid_set = gtidSetNew();
    gtidSetAdd(gtid_set, "A", 1, 1, 1);
    assert(strcmp(gtid_set->header->uuid, "A") == 0);
    assert(gtid_set->header->intervals->header->forwards[0]->start == 1);
    assert(gtid_set->header->intervals->header->forwards[0]->end == 1);
    gtidSetAdd(gtid_set, "A", 1, 2, 2);
    assert(gtid_set->header->intervals->header->forwards[0]->start == 1);
    assert(gtid_set->header->intervals->header->forwards[0]->end == 2);

    gtidSetAdd(gtid_set, "B", 1, 1, 1);
    assert(strcmp(gtid_set->header->next->uuid, "B") == 0);
    assert(gtid_set->header->next->intervals->header->forwards[0]->start == 1);
    assert(gtid_set->header->next->intervals->header->forwards[0]->end == 1);
    gtidSetFree(gtid_set);

    gtid_set = gtidSetNew();
    gtidSetAdd(gtid_set, "A", 1, 1, 1);
    gtidSetAdd(gtid_set, "A", 1, 2, 2);
    gtidSetAdd(gtid_set, "B", 1, 3, 3);
    //Add A:1 A:2 B:3 to empty gtid set
    assert(memcmp(gtid_set->header->uuid, "A\0", 1) == 0);
    assert(gtid_set->header->intervals->header->forwards[0]->start == 1);
    assert(gtid_set->header->intervals->header->forwards[0]->end == 2);
    assert(memcmp(gtid_set->header->next->uuid, "B\0", 1) == 0);
    assert(gtid_set->header->next->intervals->header->forwards[0]->start == 3);
    assert(gtid_set->header->next->intervals->header->forwards[0]->end == 3);

    size_t maxlen = gtidSetEstimatedEncodeBufferSize(gtid_set);
    char gtid_str[maxlen];
    int len = gtidSetEncode(gtid_str, maxlen, gtid_set);
    gtid_str[len] = '\0';
    //Add A:1 A:2 B:3 to empty gtid set (encode)
    assert(strcmp(gtid_str, "A:1-2,B:3") == 0);
    gtidSetFree(gtid_set);

    return 1;
}

int test_uuidGnoDecode() {
    long long gno = 0;
    int uuid_index = 0;
    char* uuid = uuidGnoDecode("ABCD:1", 6, &gno, &uuid_index);
    assert(uuid_index == 4);
    assert(strncmp(uuid, "ABCD", uuid_index) == 0);
    assert(gno == 1);
    return 1;
}

int test_gtidSetAppend() {
    gtidSet* gtid_set = gtidSetNew();
    uuidSet* uuid_set = uuidSetNew("A", 1);
    uuidSetAdd(uuid_set, 1, 2);
    gtidSetAppend(gtid_set, uuid_set);
    assert(gtid_set->header == uuid_set);
    uuidSet* uuid_set1 = uuidSetNew("A", 1);
    uuidSetAdd(uuid_set1, 3, 4);
    gtidSetAppend(gtid_set, uuid_set1);
    assert(gtid_set->header == uuid_set);
    gtidSetFree(gtid_set);
    return 1;
}

int test_gtidSetAppendGtidSet() {
    size_t maxlen = 100;
    char gtid_str[maxlen];
    int str_len;
    char* A_str,*B_str;
    gtidSet *A,*B;


    //null + (B:1:3:5:7) = (B:1:3:5:7)
    A_str = "";
    A = gtidSetDecode(A_str, strlen(A_str));
    B_str = "B:1:3:5:7";
    B = gtidSetDecode(B_str, strlen(B_str));
    gtidSetMerge(A, B);
    str_len = gtidSetEncode(gtid_str, maxlen, A);
    assert(str_len == strlen("B:1:3:5:7"));
    assert(strncmp(gtid_str, "B:1:3:5:7",str_len) == 0);
    gtidSetFree(A);
    gtidSetFree(B);

    //(B:1:3:5:7) + null = (B:1:3:5:7)
    A_str = "B:1:3:5:7";
    A = gtidSetDecode(A_str, strlen(A_str));
    B_str = "";
    B = gtidSetDecode(B_str, strlen(B_str));
    gtidSetMerge(A, B);
    str_len = gtidSetEncode(gtid_str, maxlen, A);
    assert(str_len == strlen("B:1:3:5:7"));
    assert(strncmp(gtid_str, "B:1:3:5:7",str_len) == 0);
    gtidSetFree(A);
    gtidSetFree(B);

    //(A:1:3:5:7) + (B:1:3:5:7) = (A:1:3:5:7,B:1:3:5:7)
    A_str = "A:1:3:5:7";
    A = gtidSetDecode(A_str, strlen(A_str));
    B_str = "B:1:3:5:7";
    B = gtidSetDecode(B_str, strlen(B_str));
    gtidSetMerge(A, B);
    str_len = gtidSetEncode(gtid_str, maxlen, A);
    assert(str_len == strlen("A:1:3:5:7,B:1:3:5:7"));
    assert(strncmp(gtid_str, "A:1:3:5:7,B:1:3:5:7",str_len) == 0);
    gtidSetFree(A);
    gtidSetFree(B);


    //(A:1:3:5:7) + (B:1:3:5:7,C:1:3:5:7) = (A:1:3:5:7,B:1:3:5:7,C:1:3:5:7)
    A_str = "A:1:3:5:7";
    A = gtidSetDecode(A_str, strlen(A_str));
    B_str = "B:1:3:5:7,C:1:3:5:7";
    B = gtidSetDecode(B_str, strlen(B_str));
    gtidSetMerge(A, B);
    str_len = gtidSetEncode(gtid_str, maxlen, A);
    assert(str_len == strlen("A:1:3:5:7,B:1:3:5:7,C:1:3:5:7"));
    assert(strncmp(gtid_str, "A:1:3:5:7,B:1:3:5:7,C:1:3:5:7",str_len) == 0);
    gtidSetFree(A);
    gtidSetFree(B);

    //(A:1:3:5:7) + (A:2:4:6:8) = (A:1-8)
    A_str = "A:1:3:5:7";
    A = gtidSetDecode(A_str, strlen(A_str));
    B_str = "A:2:4:6:8";
    B = gtidSetDecode(B_str, strlen(B_str));
    gtidSetMerge(A, B);
    str_len = gtidSetEncode(gtid_str, maxlen, A);
    assert(str_len == strlen("A:1-8"));
    assert(strncmp(gtid_str, "A:1-8",str_len) == 0);
    gtidSetFree(A);
    gtidSetFree(B);

    return 1;
}

int test_gtidSetInvalidArg() {
    gtidSet *gtid_set;

    gtid_set = gtidSetDecode("",0);
    assert(gtid_set != NULL && gtid_set->header == NULL);
    gtidSetFree(gtid_set);

    gtid_set = gtidSetDecode(",",1);
    assert(gtid_set == NULL);

    gtid_set = gtidSetDecode("foobar",1);
    assert(gtidSetCount(gtid_set) == 0);
    gtidSetFree(gtid_set);

    gtid_set = gtidSetDecode("A:0",3);
    assert(gtid_set == NULL);

    gtid_set = gtidSetDecode("A:2-1",5);
    assert(gtid_set == NULL);

    gtid_set = gtidSetDecode("A:3-5",5);

    assert(gtidSetAdd(gtid_set, "B", 1, 0, 0) == 0);
    assert(gtidSetAdd(gtid_set, "A", 1, 0, 0) == 0);

    assert(gtidSetAdd(gtid_set, "A", 1, 1, 0) == 0);
    assert(gtidSetMerge(gtid_set, NULL) == 0);

    assert(gtidSetAppend(gtid_set, NULL) == 0);

    assert(gtidSetFind(gtid_set, NULL, 0) == NULL);
    gtidSetFree(gtid_set);

    return 1;
}

int test_gtidStat() {
    gtidSet *gtid_set = gtidSetNew();
    uuidSet *uuid_set;
    gtidStat stat;

    uuid_set = uuidSetDecode("A:1-2:7-8",9);
    assert(uuid_set != NULL);
    uuidSetGetStat(uuid_set, &stat);
    assert(stat.uuid_count == 1 && stat.used_memory == 3*GTID_INTERVAL_MEMORY
            && stat.gap_count == 2 && stat.gno_count == 4);

    gtidSetAppend(gtid_set, uuid_set);
    gtidSetGetStat(gtid_set, &stat);
    assert(stat.uuid_count == 1 && stat.used_memory == 3*GTID_INTERVAL_MEMORY
            && stat.gap_count == 2 && stat.gno_count == 4);

    uuid_set = uuidSetDecode("B:3-4:10-11",11);
    uuidSetGetStat(uuid_set, &stat);
    assert(stat.uuid_count == 1 && stat.used_memory == 3*GTID_INTERVAL_MEMORY
            && stat.gap_count == 2 && stat.gno_count == 4);

    gtidSetAppend(gtid_set, uuid_set);
    gtidSetGetStat(gtid_set, &stat);
    assert(stat.uuid_count == 2 && stat.used_memory == 6*GTID_INTERVAL_MEMORY
            && stat.gap_count == 4 && stat.gno_count == 8);

    gtidSetFree(gtid_set);
    return 1;
}

int test_gtidSetDiff() {
    size_t len;
    char buf[128];
    gtidSet *gtid_set = gtidSetNew(), *src = gtidSetNew();

    assert(gtidSetDiff(gtid_set,src) == 0);
    len = gtidSetEncode(buf,sizeof(buf),gtid_set);
    assert(strncmp(buf,"",len) == 0);

    gtidSetAdd(gtid_set,"A",1,10,10);
    gtidSetAdd(gtid_set,"B",1,10,10);
    len = gtidSetEncode(buf,sizeof(buf),gtid_set);
    assert(strncmp(buf,"A:10,B:10",len) == 0);

    gtidSetDiff(gtid_set,src);
    len = gtidSetEncode(buf,sizeof(buf),gtid_set);
    assert(strncmp(buf,"A:10,B:10",len) == 0);

    gtidSetAdd(src,"A",1,10,10);
    gtidSetDiff(gtid_set,src);
    len = gtidSetEncode(buf,sizeof(buf),gtid_set);
    assert(strncmp(buf,"B:10",len) == 0);

    gtidSetAdd(src,"B",1,10,10);
    gtidSetDiff(gtid_set,src);
    len = gtidSetEncode(buf,sizeof(buf),gtid_set);
    assert(strncmp(buf,"",len) == 0);

    gtidSetFree(gtid_set);
    gtidSetFree(src);
    return 1;
}

int test_gtidSegment() {
    gtidSegment *seg = gtidSegmentNew();
    gtidSegmentReset(seg,"A",1,1,100);
    assert(seg->uuid_len == 1 && !memcmp(seg->uuid,"A",1));
    gtidSegmentAppend(seg,100);
    gtidSegmentAppend(seg,200);
    assert(seg->ngno = 2);
    assert(seg->deltas[0] == 0);
    assert(seg->deltas[1] == 100);
    gtidSegmentReset(seg,"B",1,1,1000);
    assert(seg->uuid_len == 1 && !memcmp(seg->uuid,"B",1));
    assert(seg->base_gno == 1 && seg->base_offset == 1000);
    assert(seg->ngno == 0);
    gtidSegmentFree(seg);
    return 1;
}

int test_gtidSeqAppend() {
    gtidSeq *seq = gtidSeqCreate();
    gtidSeqAppend(seq,"A",1,1,100);
    gtidSeqAppend(seq,"A",1,2,200);
    assert(seq->nsegment == 1 && seq->firstseg->ngno == 2);
    gtidSeqAppend(seq,"B",1,1,100000);
    assert(seq->nsegment == 2 && seq->lastseg->ngno == 1);
    gtidSeqAppend(seq,"B",1,2,200000);
    assert(seq->nsegment == 3 && seq->lastseg->ngno == 1);

    for (int i = 1; i < 10001; i++) {
        gtidSeqAppend(seq,"C",1,i,200000+i*10);
    }
    gtidSeqDestroy(seq);
    return 1;
}

int test_gtidSeqTrim() {
    gtidSeq *seq = gtidSeqCreate();

    gtidSeqAppend(seq,"A",1,1,100);
    gtidSeqAppend(seq,"A",1,2,200);
    gtidSeqAppend(seq,"B",1,1,100000);
    gtidSeqAppend(seq,"B",1,2,200000);
    gtidSeqAppend(seq,"B",1,3,200300);
    gtidSeqAppend(seq,"B",1,4,200400);
    gtidSeqAppend(seq,"B",1,5,200500);
    gtidSeqAppend(seq,"C",1,1,300000);
    assert(seq->nsegment == 4 && seq->nfreeseg == 0);

    gtidSeqTrim(seq,500);
    assert(seq->nsegment == 3 && seq->nfreeseg == 1);
    gtidSeqTrim(seq,100000);
    assert(seq->nsegment == 3 && seq->nfreeseg == 1);
    gtidSeqTrim(seq,200000);
    assert(seq->nsegment == 2 && seq->nfreeseg == 2);
    gtidSeqTrim(seq,200100);
    assert(seq->nsegment == 2 && seq->nfreeseg == 2);
    gtidSeqTrim(seq,200500);
    assert(seq->nsegment == 2 && seq->nfreeseg == 2);
    gtidSeqTrim(seq,200501);
    assert(seq->nsegment == 1 && seq->nfreeseg == 2);

    gtidSeqAppend(seq,"B",1,10,300100);
    assert(seq->nsegment == 2 && seq->nfreeseg == 1); /* reuse and reset segment. */
    gtidSeqAppend(seq,"D",1,1,300200);
    assert(seq->nsegment == 3 && seq->nfreeseg == 0); /* reuse segment without reset */

    gtidSeqTrim(seq,400000);
    assert(seq->nsegment == 0 && seq->nfreeseg == 1);

    gtidSeqDestroy(seq);
    return 1;
}

int test_gtidSeqXsync() {
    char buf[64];
    size_t len;
    long long offset;
    gtidSet *req, *cont;
    gtidSeq *seq = gtidSeqCreate();

    gtidSeqAppend(seq,"A",1,100,100000);
    gtidSeqAppend(seq,"A",1,101,100100);

    gtidSeqAppend(seq,"B",1,100,200000);

    gtidSeqAppend(seq,"B",1,101,300100);
    gtidSeqAppend(seq,"B",1,102,300200);
    gtidSeqAppend(seq,"B",1,103,300300);

    req = gtidSetNew();
    offset = gtidSeqXsync(seq,req,&cont);
    assert(offset == 100000);
    len = gtidSetEncode(buf,sizeof(buf),cont);
    assert(len == 19 && strncmp(buf,"B:100-103,A:100-101",len) == 0);
    gtidSetFree(cont);
    gtidSetFree(req);

    req = gtidSetNew();
    gtidSetAdd(req,"B",1,1,99);
    gtidSetAdd(req,"C",1,1,50);
    offset = gtidSeqXsync(seq,req,&cont);
    len = gtidSetEncode(buf,sizeof(buf),cont);
    assert(offset == 100000);
    assert(len == 19 && strncmp(buf,"B:100-103,A:100-101",len) == 0);
    gtidSetFree(cont);
    gtidSetFree(req);

    req = gtidSetNew();
    gtidSetAdd(req,"B",1,1,100);
    gtidSetAdd(req,"C",1,1,50);
    offset = gtidSeqXsync(seq,req,&cont);
    assert(offset == 300100);
    len = gtidSetEncode(buf,sizeof(buf),cont);
    assert(len == 9 && strncmp(buf,"B:101-103",len) == 0);
    gtidSetFree(cont);
    gtidSetFree(req);

    req = gtidSetNew();
    gtidSetAdd(req,"A",1,1,100);
    gtidSetAdd(req,"B",1,1,50);
    offset = gtidSeqXsync(seq,req,&cont);
    assert(offset == 100100);
    len = gtidSetEncode(buf,sizeof(buf),cont);
    assert(len == 15 && strncmp(buf,"B:100-103,A:101",len) == 0);
    gtidSetFree(cont);
    gtidSetFree(req);

    req = gtidSetNew();
    gtidSetAdd(req,"A",1,1,50);
    gtidSetAdd(req,"B",1,1,103);
    gtidSetAdd(req,"C",1,1,50);
    offset = gtidSeqXsync(seq,req,&cont);
    assert(offset == -1);
    len = gtidSetEncode(buf,sizeof(buf),cont);
    assert(len == 0 && strncmp(buf,"",len) == 0);
    gtidSetFree(cont);
    gtidSetFree(req);

    gtidSeqTrim(seq,300200);

    req = gtidSetNew();
    gtidSetAdd(req,"A",1,1,50);
    gtidSetAdd(req,"B",1,1,101);
    gtidSetAdd(req,"C",1,1,50);
    offset = gtidSeqXsync(seq,req,&cont);
    assert(offset == 300200);
    len = gtidSetEncode(buf,sizeof(buf),cont);
    assert(len == 9 && strncmp(buf,"B:102-103",len) == 0);
    gtidSetFree(cont);
    gtidSetFree(req);

    gtidSeqDestroy(seq);
    return 1;
}

int test_gtidSeqPsync() {
    char buf[64];
    size_t len;
    gtidSet *gtid_set;
    gtidSeq *seq = gtidSeqCreate();

    gtidSeqAppend(seq,"A",1,100,100000);
    gtidSeqAppend(seq,"A",1,101,100100);

    gtidSeqAppend(seq,"B",1,100,200000);

    gtidSeqAppend(seq,"B",1,101,300100);
    gtidSeqAppend(seq,"B",1,102,300200);
    gtidSeqAppend(seq,"B",1,103,300300);

    gtid_set = gtidSeqPsync(seq,400000);
    len = gtidSetEncode(buf,sizeof(buf),gtid_set);
    assert(len == 0 && !strncmp(buf,"",len));
    gtidSetFree(gtid_set);

    gtid_set = gtidSeqPsync(seq,300300);
    len = gtidSetEncode(buf,sizeof(buf),gtid_set);
    assert(len == 5 && !strncmp(buf,"B:103",len));
    gtidSetFree(gtid_set);

    gtid_set = gtidSeqPsync(seq,300250);
    len = gtidSetEncode(buf,sizeof(buf),gtid_set);
    assert(len == 5 && !strncmp(buf,"B:103",len));
    gtidSetFree(gtid_set);

    gtid_set = gtidSeqPsync(seq,300200);
    len = gtidSetEncode(buf,sizeof(buf),gtid_set);
    assert(len == 9 && !strncmp(buf,"B:102-103",len));
    gtidSetFree(gtid_set);

    gtid_set = gtidSeqPsync(seq,300150);
    len = gtidSetEncode(buf,sizeof(buf),gtid_set);
    assert(len == 9 && !strncmp(buf,"B:102-103",len));
    gtidSetFree(gtid_set);

    gtid_set = gtidSeqPsync(seq,300100);
    len = gtidSetEncode(buf,sizeof(buf),gtid_set);
    assert(len == 9 && !strncmp(buf,"B:101-103",len));
    gtidSetFree(gtid_set);

    gtid_set = gtidSeqPsync(seq,300050);
    len = gtidSetEncode(buf,sizeof(buf),gtid_set);
    assert(len == 9 && !strncmp(buf,"B:101-103",len));
    gtidSetFree(gtid_set);

    gtid_set = gtidSeqPsync(seq,200000);
    len = gtidSetEncode(buf,sizeof(buf),gtid_set);
    assert(len == 9 && !strncmp(buf,"B:100-103",len));
    gtidSetFree(gtid_set);

    gtid_set = gtidSeqPsync(seq,100100);
    len = gtidSetEncode(buf,sizeof(buf),gtid_set);
    assert(len == 15 && !strncmp(buf,"B:100-103,A:101",len));
    gtidSetFree(gtid_set);

    gtid_set = gtidSeqPsync(seq,100000);
    len = gtidSetEncode(buf,sizeof(buf),gtid_set);
    assert(len == 19 && !strncmp(buf,"B:100-103,A:100-101",len));
    gtidSetFree(gtid_set);

    gtid_set = gtidSeqPsync(seq,90000);
    len = gtidSetEncode(buf,sizeof(buf),gtid_set);
    assert(len == 19 && !strncmp(buf,"B:100-103,A:100-101",len));
    gtidSetFree(gtid_set);

    gtidSeqDestroy(seq);

    return 1;
}

int __failed_tests = 0;
int __test_num = 0;
#define test_cond(descr,_c) do { \
    __test_num++; printf("%d - %s: ", __test_num, descr); \
    if(_c) printf("PASSED\n"); else {printf("FAILED\n"); __failed_tests++;} \
} while(0);
#define test_report() do { \
    printf("%d tests, %d passed, %d failed\n", __test_num, \
                    __test_num-__failed_tests, __failed_tests); \
    if (__failed_tests) { \
        printf("=== WARNING === We have failed tests here...\n"); \
        exit(1); \
    } \
} while(0);

int test_api(void) {
    {
        test_cond("gtidIntervalNew function",
                test_gtidIntervalNew() == 1);
        test_cond("gtidIntervalDecode function",
                test_gtidIntervalDecode() == 1);
        test_cond("gtidIntervalEncode function",
                test_gtidIntervalEncode() == 1);
        test_cond("uuidSetNew function",
                test_uuidSetNew() == 1);
        test_cond("uuidSetDup function",
                test_uuidSetDup() == 1);
        test_cond("uuidSetAdd function",
                test_uuidSetAdd() == 1);
        test_cond("uuidSetAddInterval function",
                test_uuidSetAddInterval() == 1);
        test_cond("uuidSetAdd Chaos",
                test_uuidSetAddChaos() == 1);
        test_cond("uuidSetRemove function",
                test_uuidSetRemove() == 1);
        test_cond("uuidSetRemove Chaos",
                test_uuidSetRemoveChaos() == 1);
        test_cond("uuidSetDiff function",
                test_uuidSetDiff() == 1);
        test_cond("uuidSetContains function",
                test_uuidSetContains() == 1);
        test_cond("uuidSetNext function",
                test_uuidSetNext() == 1);
        test_cond("uuidSetNextEncode function",
                test_uuidSetNextEncode() == 1);
        test_cond("uuidSetDecode function",
                test_uuidSetDecode() == 1);
        test_cond("uuidSetEstimatedEncodeBufferSize function",
                test_uuidSetEstimatedEncodeBufferSize() == 1);
        test_cond("uuidSetEncode function",
                test_uuidSetEncode() == 1);
        test_cond("uuidSet api with invalid args",
            test_uuidSetInvalidArg() == 1);
        test_cond("gtidSetNew function",
                test_gtidSetNew() == 1);
        test_cond("gtidSetDup function",
                test_gtidSetDup() == 1);
        test_cond("gtidSetDecode function",
                test_gtidSetDecode() == 1);
        test_cond("gtidSetEstimatedEncodeBufferSize function",
                test_gtidSetEstimatedEncodeBufferSize() == 1);
        test_cond("gtidSetEncode function",
                test_gtidSetEncode() == 1);
        test_cond("gtidSetAdd function",
                test_gtidSetAdd() == 1);
        test_cond("gtidSetAppend function",
                test_gtidSetAppend() == 1);
        test_cond("gtidSetFindUuidSet function",
                test_gtidSetFind() == 1);
        test_cond("uuidDecode function",
                test_uuidGnoDecode() == 1);
        test_cond("gtidSetAppendGtidSet function",
            test_gtidSetAppendGtidSet() == 1);
        test_cond("gtidSet api with invalid args",
            test_gtidSetInvalidArg() == 1);
        test_cond("gtid Stat",
            test_gtidStat() == 1);
        test_cond("gtidSetDiff function ",
            test_gtidSetDiff() == 1);
        test_cond("gtidSegment",
            test_gtidSegment() == 1);
        test_cond("gtidSeqAppend function",
            test_gtidSeqAppend() == 1);
        test_cond("gtidSeqTrim function",
            test_gtidSeqTrim() == 1);
        test_cond("gtidSeqXsync function",
            test_gtidSeqXsync() == 1);
        test_cond("gtidSeqPsync function",
            test_gtidSeqPsync() == 1);

    } test_report()
    return 1;
}

int main(void) {

    test_api();
    return 0;
}
