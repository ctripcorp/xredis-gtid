#ifndef __REDIS_CTRIP_GTID_H
#define __REDIS_CTRIP_GTID_H
#include <stdio.h>
typedef struct gtidInterval gtidInterval;
typedef struct uuidSet uuidSet;
typedef struct gtidSet gtidSet;

 /* start from 1 */
typedef long long int rpl_gno;

struct gtidInterval {
    rpl_gno gno_start;
    rpl_gno gno_end;
    gtidInterval *next;
};

struct uuidSet {
    gtidInterval* intervals;
    uuidSet *next;
    char* rpl_sid;
};

struct gtidSet {
    uuidSet* uuid_sets;
    uuidSet* tail;
};

gtidInterval *gtidIntervalNew(rpl_gno);
gtidInterval *gtidIntervalDump(gtidInterval* gtid_interval);
void gtidIntervalFree(gtidInterval*);
gtidInterval *gtidIntervalNewRange(rpl_gno, rpl_gno);
gtidInterval *gtidIntervalDecode(char* interval_str, size_t interval_len);
size_t gtidIntervalEncode(gtidInterval* interval, char* buf);

uuidSet *uuidSetNew(const char*, size_t rpl_sid_len, rpl_gno);
void uuidSetFree(uuidSet*);
uuidSet *uuidSetDump(uuidSet* uuid_set);
uuidSet *uuidSetNewRange(const char*, size_t rpl_sid_len, rpl_gno, rpl_gno);
uuidSet *uuidSetDecode(char* uuid_set_sds, int len);

size_t uuidSetEstimatedEncodeBufferSize(uuidSet* uuid_set);
size_t uuidSetEncode(uuidSet* uuid_set, char* buf);
int uuidSetAdd(uuidSet* uuid_set, rpl_gno gno);
void uuidSetRaise(uuidSet* uuid_set, rpl_gno gno);
int uuidSetContains(uuidSet* uuid_set, rpl_gno gno);
rpl_gno uuidSetNext(uuidSet* uuid_set, int updateBeforeReturn);
size_t uuidSetNextEncode(uuidSet* uuid_set, int updateBeforeReturn, char* buf);
int uuidSetAppendUuidSet(uuidSet* uuid_set, uuidSet* other);
int uuidSetAddGtidInterval(uuidSet* uuid_set, gtidInterval* interval);

gtidSet* gtidSetNew();
void gtidSetFree(gtidSet*);
gtidSet *gtidSetDecode(char* gtid_set, size_t gtid_set_len);
size_t gtidSetEstimatedEncodeBufferSize(gtidSet* gtid_set);
size_t gtidSetEncode(gtidSet* gtid_set, char* buf);
int gtidSetAdd(gtidSet* gtid_set, const char* uuid, size_t rpl_sid_len, rpl_gno gno);
uuidSet* gtidSetFindUuidSet(gtidSet* gtid_set,const char* rpl_sid, size_t len);
char* uuidDecode(char* src, size_t src_len, long long* gno, int* rpl_sid_len);
void gtidSetRaise(gtidSet*, const char*, size_t, rpl_gno);
void gtidSetAppendGtidSet(gtidSet* gtid_set, gtidSet* other);
#endif  /* __REDIS_CTRIP_GTID_H */
