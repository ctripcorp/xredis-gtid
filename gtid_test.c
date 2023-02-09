#include "gtid.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "testhelp.h"
#include "limits.h"
#include "gtid_malloc.h"
#include "util.h"

/* 43 = 21(long long) + 1(-) + 21(long long) */
#define INTERVAL_ENCODE_MAX_LEN 43

#define assert(e) do {							        \
    if (!(e)) {				                            \
        printf(						                    \
                "%s:%d: Failed assertion: \"%s\"\n",	\
                __FILE__, __LINE__, #e);				\
        return 0;						                \
    }								                    \
} while (0)

gtidIntervalNode *gtidIntervalNodeNew(int level, gno_t start, gno_t end);
void gtidIntervalNodeFree(gtidIntervalNode* interval);
ssize_t gtidIntervalEncode(char *buf, size_t maxlen, gno_t start, gno_t end);
int gtidIntervalDecode(char* interval_str, size_t len, gno_t *pstart, gno_t *pend);

int test_gtidIntervalNew() {
    int level = 4, i;
    gtidIntervalNode* interval = gtidIntervalNodeNew(level,GNO_INITIAL,GNO_INITIAL);
    assert(interval->start == GNO_INITIAL);
    assert(interval->end == GNO_INITIAL);
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
    assert(uuidSetDecode("foobar",6) == NULL);

    uuid_set = uuidSetDecode("A:2-1",5);
    assert(uuidSetCount(uuid_set) == 0);
    uuidSetFree(uuid_set);

    uuid_set = uuidSetDecode("A:foobar",8);
    assert(uuidSetCount(uuid_set) == 0);
    uuidSetFree(uuid_set);

    uuid_set = uuidSetDecode("A:foobar",8);
    uuidSetFree(uuid_set);
    uuid_set = uuidSetDecode("A:2-1",5);
    uuidSetFree(uuid_set);

    uuid_set = uuidSetDecode("A:3-10",5);
    assert(uuidSetContains(uuid_set,0) == 0);
    assert(uuidSetAdd(uuid_set,0,0) == 0);
    assert(uuidSetAdd(uuid_set,2,1) == 0);
    assert(uuidSetRaise(uuid_set,0) == 0);
    uuidSetFree(uuid_set);

    return 1;
}

int test_uuidSetAddInterval() {
    size_t maxlen = 100;
    char* decode_str;
    char buf[maxlen];
    size_t buf_len;
    uuidSet* uuid_set;
    gno_t start, end;

    //(0) + (1-2) = (1-2)
    decode_str = "A:0";
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

int test_uuidSetRaise() {
    size_t maxlen = 100, len;
    char buf[maxlen];
    uuidSet* uuid_set;
    gtidIntervalNode *node;
    char* uuidset_str;

    uuid_set = uuidSetNew("A", 1);
    uuidSetAdd(uuid_set, 5, 5);
    uuidSetRaise(uuid_set, 1);
    len = uuidSetEncode(buf, maxlen, uuid_set);
    assert(len == 5); //A:1:5
    assert(strncmp(buf, "A:1:5", len) == 0);
    uuidSetFree(uuid_set);

    uuid_set = uuidSetNew("A", 1);
    uuidSetAdd(uuid_set, 5, 5);
    uuidSetRaise(uuid_set, 6);
    node = uuid_set->intervals->header->forwards[0];
    assert(node->start == 1 );
    assert(node->end == 6);
    assert(node->forwards[0] == NULL);
    assert(memcmp(uuid_set->uuid, "A\0", 2) == 0);
    uuidSetFree(uuid_set);

    uuid_set = uuidSetNew("A", 1);
    uuidSetAdd(uuid_set, 5, 5);
    uuidSetAdd(uuid_set, 7, 7);
    uuidSetRaise(uuid_set, 6);
    node = uuid_set->intervals->header->forwards[0];
    assert(node->start == 1);
    assert(node->end == 7);
    assert(node->forwards[0] == NULL);
    assert(memcmp(uuid_set->uuid, "A\0", 2) == 0);
    uuidSetFree(uuid_set);

    uuid_set = uuidSetNew("A", 1);
    uuidSetAdd(uuid_set, 5, 5);
    uuidSetRaise(uuid_set, 3);
    node = uuid_set->intervals->header->forwards[0];
    assert(node->start == 1);
    assert(node->end == 3);
    assert(node->forwards[0]->start == 5);
    assert(node->forwards[0]->end == 5);
    assert(node->forwards[0]->forwards[0] == NULL);
    assert(memcmp(uuid_set->uuid, "A\0", 2) == 0);
    uuidSetFree(uuid_set);

    uuidset_str = "A:1:3:5-6:11-14:19-20";
    uuid_set = uuidSetDecode(uuidset_str, strlen(uuidset_str));
    uuidSetRaise(uuid_set, 30);
    node = uuid_set->intervals->header->forwards[0];
    assert(node->start == 1);
    assert(node->end == 30);
    assert(node->forwards[0] == NULL);
    assert(memcmp(uuid_set->uuid, "A\0", 2) == 0);

    uuidSetFree(uuid_set);
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
    uuidSetRaise(uuid_set, 6);
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
    uuidSetAdd(uuid_set, 1, 5);
    assert(6 == uuidSetNext(uuid_set, 1));
    uuidSetRaise(uuid_set, 8);
    assert(9 == uuidSetNext(uuid_set, 1));
    uuidSetFree(uuid_set);

    uuid_set = uuidSetNew("A", 1);
    uuidSetAdd(uuid_set, 5, 5);
    uuidSetAdd(uuid_set, 6, 6);
    //next of A:5-6 is 1
    assert(uuidSetNext(uuid_set, 0) == 1);
    //add 6 to 5-6 , return 0
    assert(uuidSetAdd(uuid_set, 6, 6) == 0);

    uuidSetNext(uuid_set, 1);

    uuidset_len = uuidSetEncode(uuidset_str, maxlen, uuid_set);
    uuidset_str[uuidset_len] = '\0';
    //update next of A:5-6, will be A:1:5-6
    assert(strcmp(uuidset_str, "A:1:5-6") == 0);

    uuidSetNext(uuid_set, 1);
    uuidset_len = uuidSetEncode(uuidset_str, maxlen, uuid_set);
    uuidset_str[uuidset_len] = '\0';
    //update next of A:1:5-6, will be A:1-2:5-6
    assert(strcmp(uuidset_str, "A:1-2:5-6") == 0);

    uuidSetNext(uuid_set, 1);
    uuidSetNext(uuid_set, 1);
    uuidSetNext(uuid_set, 1);
    uuidset_len = uuidSetEncode(uuidset_str, maxlen, uuid_set);
    uuidset_str[uuidset_len] = '\0';
    //"update next 3 times of A:1-2:5-6, will be A:1-7"
    assert(strcmp(uuidset_str, "A:1-7") == 0);
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
    uuidSetRaise(uuid_set, 8);
    len = uuidSetNextEncode(buf, maxlen, uuid_set, 1);
    assert(3 == len);
    assert(strncmp(buf, "A:9", len) == 0);
    uuidSetFree(uuid_set);

    char *gtidset = "A:1-7,B:9:11-13:20";
    gtidSet* gtid_set = gtidSetDecode(gtidset, strlen(gtidset));
    uuidSet* B = gtidSetFind(gtid_set, "B", 1);
    next_len = uuidSetNextEncode(next, maxlen, B, 1);
    //B next of A:1-7,B:9:11-13:20 & Update
    assert(strncmp("B:1", next, next_len) == 0);
    assert(uuidSetContains(B, 1));

    next_len = uuidSetNextEncode(next, maxlen, B, 1);
    next[next_len] = '\0';
    //B next of A:1-7,B:1:9:11-13:20 & Update
    assert(strcmp("B:2", next) == 0);
    assert(uuidSetContains(B, 2));

    next_len = uuidSetNextEncode(next, maxlen, B, 1);
    next_len = uuidSetNextEncode(next, maxlen, B, 1);
    next_len = uuidSetNextEncode(next, maxlen, B, 1);
    next_len = uuidSetNextEncode(next, maxlen, B, 1);
    next_len = uuidSetNextEncode(next, maxlen, B, 1);
    next[next_len] = '\0';
    long long gno = 0;
    int sid_len = 0;
    uuidGnoDecode(next, next_len, &gno, &sid_len);
    //B next of A:1-7,B:1-2:9:11-13:20 5 times & Update
    assert(strcmp("B:7", next) == 0);
    assert(gno == 7);
    assert(uuidSetContains(B, gno));

    next_len = uuidSetNextEncode(next, maxlen, B, 1);
    //B next of A:1-7,B:1-7:9:11-13:20 & Update
    assert(strcmp("B:8", next) == 0);
    assert(uuidSetContains(B, gno));

    next_len = uuidSetNextEncode(next, maxlen, B, 1);
    //"B next of A:1-7,B:1-9:11-13:20 & Update"
    assert(strcmp("B:10", next) == 0);
    assert(uuidSetContains(B, gno));

    next_len = uuidSetNextEncode(next, maxlen, B, 1);
    //B next of A:1-7,B:1-13:20 & Update
    assert(strcmp("B:14", next) == 0);
    assert(uuidSetContains(B, 14));
    gtidSetFree(gtid_set);
    return 1;
}

#define GTID_INTERVAL_MEMORY (sizeof(gtidIntervalNode) + sizeof(gtidIntervalNode*))

int test_uuidSetPurge() {
    char *repr;
    uuidSet *uuid_set;
    uuidSetPurged purged;

    uuid_set = uuidSetNew("A",1);
    uuidSetPurge(uuid_set,0,&purged);
    assert(purged.node_count == 0 && purged.end == 0);

    uuidSetAdd(uuid_set, 3, 5);
    uuidSetPurge(uuid_set,0,&purged);
    assert(purged.node_count == 0 && purged.end == 0);
    assert(uuidSetCount(uuid_set) == 3);

    uuidSetFree(uuid_set);

    repr = "A:1:3:5:7:9:11:13:15:17:19";
    uuid_set = uuidSetDecode(repr, strlen(repr));

    uuidSetPurge(uuid_set, 20*GTID_INTERVAL_MEMORY, &purged);
    assert(purged.node_count == 0 && purged.end == 0);
    assert(uuidSetCount(uuid_set) == 10);

    uuidSetPurge(uuid_set, 10*GTID_INTERVAL_MEMORY, &purged);
    assert(purged.node_count == 0 && purged.end == 0);
    assert(uuidSetCount(uuid_set) == 10);

    uuidSetPurge(uuid_set, 5*GTID_INTERVAL_MEMORY, &purged);
    assert(purged.node_count == 5 && purged.end == 11);
    assert(uuidSetCount(uuid_set) == 15);

    uuidSetPurge(uuid_set, 5*GTID_INTERVAL_MEMORY, &purged);
    assert(purged.node_count == 0 && purged.end == 0);
    assert(uuidSetCount(uuid_set) == 15);

    uuidSetPurge(uuid_set, 0, &purged);
    assert(purged.node_count == 4 && purged.end == 19);
    assert(uuidSetCount(uuid_set) == 19);

    uuidSetFree(uuid_set);

    return 1;
}

int test_gtidSetNew() {
    gtidSet* gtid_set = gtidSetNew();
    assert(gtid_set->header == NULL);
    assert(gtid_set->tail == NULL);
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
    //encode & decode A:1-7,B:9:11-13:20 string len equal
    assert(len == strlen(gtid_set_str) - 3);
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

    gtidSetAdd(gtid_set, "A", 1, 1);
    len = gtidSetEncode(gtid_str, maxlen, gtid_set);
    assert(len == 3);
    assert(strncmp(gtid_str, "A:1", len) == 0);
    gtidSetAdd(gtid_set, "B", 1, 1);
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
    gtidSetAdd(gtid_set, "A", 1, 1);
    assert(strcmp(gtid_set->header->uuid, "A") == 0);
    assert(gtid_set->header->intervals->header->forwards[0]->start == 1);
    assert(gtid_set->header->intervals->header->forwards[0]->end == 1);
    gtidSetAdd(gtid_set, "A", 1, 2);
    assert(gtid_set->header->intervals->header->forwards[0]->start == 1);
    assert(gtid_set->header->intervals->header->forwards[0]->end == 2);

    gtidSetAdd(gtid_set, "B", 1, 1);
    assert(strcmp(gtid_set->header->next->uuid, "B") == 0);
    assert(gtid_set->header->next->intervals->header->forwards[0]->start == 1);
    assert(gtid_set->header->next->intervals->header->forwards[0]->end == 1);
    gtidSetFree(gtid_set);

    gtid_set = gtidSetNew();
    gtidSetAdd(gtid_set, "A", 1, 1);
    gtidSetAdd(gtid_set, "A", 1, 2);
    gtidSetAdd(gtid_set, "B", 1, 3);
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

int test_gtidSetRaise() {
    char* gtid_set_str = "A:1:3:5:7";
    gtidSet* gtid_set = gtidSetDecode(gtid_set_str, strlen(gtid_set_str));
    gtidSetRaise(gtid_set, "A", 1, 10);
    assert(gtid_set->header->intervals->header->forwards[0]->start == 1);
    assert(gtid_set->header->intervals->header->forwards[0]->end == 10);
    gtidSetFree(gtid_set);
    gtid_set = gtidSetNew();
    gtidSetRaise(gtid_set, "A", 1, 1);
    assert(strcmp(gtid_set->header->uuid, "A") == 0);
    assert(gtid_set->header->intervals->header->forwards[0]->start == 1);
    assert(gtid_set->header->intervals->header->forwards[0]->end == 1);
    gtidSetFree(gtid_set);

    gtid_set_str = "A:1-2,B:3";
    gtid_set = gtidSetDecode(gtid_set_str, strlen(gtid_set_str));
    gtidSetAdd(gtid_set, "B", 1, 7);
    gtidSetRaise(gtid_set, "A", 1, 5);
    gtidSetRaise(gtid_set, "B", 1, 5);
    gtidSetRaise(gtid_set, "C", 1, 10);
    size_t maxlen = 100;
    char gtid_str[maxlen];
    int len = gtidSetEncode(gtid_str, maxlen, gtid_set);
    gtid_str[len] = '\0';
    //Raise A & B to 5, C to 10, towards C:1-10,B:3:7,A:1-2
    assert(strcmp(gtid_str, "A:1-5,B:1-5:7,C:1-10") == 0);
    gtidSetFree(gtid_set);

    /* raise 0*/
    gtid_set = gtidSetNew();
    gtidSetAdd(gtid_set, "A", 1, 0);
    len = gtidSetEncode(gtid_str, maxlen, gtid_set);
    gtid_str[len] = '\0';
    assert(strcmp(gtid_str, "A:0") == 0);
    gtidSetRaise(gtid_set, "A", 1, 0);
    len = gtidSetEncode(gtid_str, maxlen, gtid_set);
    gtid_str[len] = '\0';
    assert(strcmp(gtid_str, "A:0") == 0);
    gtidSetFree(gtid_set);

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

    //(A:1:3:5:7) + (B:1:3:5:7) = (A:1:3:5:7,B:1:3:5:7)
    A_str = "A:1:3:5:7";
    A = gtidSetDecode(A_str, strlen(A_str));
    B_str = "B:1:3:5:7";
    B = gtidSetDecode(B_str, strlen(B_str));
    gtidSetMerge(A, B);
    str_len = gtidSetEncode(gtid_str, maxlen, A);
    assert(str_len == strlen("A:1:3:5:7,B:1:3:5:7"));
    assert(strncmp(gtid_str, "A:1:3:5:7,B:1:3:5:7",str_len) == 0);
    /* str_len = gtidSetEncode(gtid_str, maxlen, B); */
    /* assert(str_len == strlen("B:1:3:5:7")); */
    /* assert(strncmp(gtid_str, "B:1:3:5:7",str_len) == 0); */
    gtidSetFree(A);


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

    return 1;
}

int test_gtidSetInvalidArg() {
    gtidSet *gtid_set;

    gtid_set = gtidSetDecode("",0);
    assert(gtid_set != NULL && gtid_set->header == NULL);
    gtidSetFree(gtid_set);

    gtid_set = gtidSetDecode(",",1);
    assert(gtid_set != NULL && gtid_set->header == NULL);
    gtidSetFree(gtid_set);

    gtid_set = gtidSetDecode("foobar",1);
    assert(gtid_set != NULL && gtid_set->header == NULL);
    gtidSetFree(gtid_set);

    gtid_set = gtidSetDecode("A:0",3);
    assert(gtid_set != NULL && gtid_set->header != NULL);
    assert(uuidSetCount(gtid_set->header) == 0);
    gtidSetFree(gtid_set);

    gtid_set = gtidSetDecode("A:2-1",5);
    assert(gtid_set != NULL && gtid_set->header != NULL);
    assert(uuidSetCount(gtid_set->header) == 0);
    gtidSetFree(gtid_set);

    gtid_set = gtidSetDecode("A:3-5",5);

    assert(gtidSetAdd(gtid_set, "B", 1, 1) == 0); /* uuid not found */
    assert(gtidSetAdd(gtid_set, "A", 1, 0) == 0);

    assert(gtidSetRaise(gtid_set, "A", 1, 0) == 0);
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

int test_gtidSetRemove() {
    gtidSet *gtid_set = gtidSetNew();
    uuidSet *uuid_set;

    assert(gtidSetRemove(gtid_set, "A", 1) == 0);
    uuid_set = uuidSetDecode("A:1-2:7-8",9);
    gtidSetAppend(gtid_set, uuid_set);
    uuid_set = uuidSetDecode("B:3-4:10-11",11);
    gtidSetAppend(gtid_set, uuid_set);

    assert(gtidSetRemove(gtid_set, "C", 1) == 0);
    assert(gtidSetRemove(gtid_set, "A", 1) == 1);
    assert(gtidSetRemove(gtid_set, "B", 1) == 1);
    assert(gtid_set->header == NULL && gtid_set->tail == NULL);
    assert(gtidSetRemove(gtid_set, "A", 1) == 0);

    gtidSetFree(gtid_set);
    return 1;
}

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
        test_cond("uuidSetAdd function",
                test_uuidSetAdd() == 1);
        test_cond("uuidSetAddInterval function",
                test_uuidSetAddInterval() == 1);
        test_cond("uuidSetAdd Chaos",
                test_uuidSetAddChaos() == 1);
        test_cond("uuidSetRaise function",
                test_uuidSetRaise() == 1);
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
        test_cond("uuidSetPurge function",
                test_uuidSetPurge() == 1);
        test_cond("gtidSetNew function",
                test_gtidSetNew() == 1);
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
        test_cond("gtidSetRaise function",
                test_gtidSetRaise() == 1);
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
        test_cond("gtidSetRemove function ",
            test_gtidSetRemove() == 1);

    } test_report()
    return 1;
}

int main(void) {

    test_api();
    return 0;
}
