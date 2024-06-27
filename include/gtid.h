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

#ifndef __REDIS_CTRIP_GTID_H
#define __REDIS_CTRIP_GTID_H

#include <stdlib.h>
#include <stdint.h>
#include <sys/types.h>

 /* gno start from 1 */
#define GNO_INITIAL 1

typedef long long gno_t;

typedef struct gtidIntervalNode {
    int level;
    gno_t start;
    gno_t end;
    struct gtidIntervalNode *forwards[];
} gtidIntervalNode;

typedef struct gtidIntervalSkipList {
    struct gtidIntervalNode *header;
    struct gtidIntervalNode *tail;
    size_t node_count;
    gno_t gno_count;
    int level;
} gtidIntervalSkipList;

typedef struct uuidSet {
    char* uuid;
    size_t uuid_len;
    struct gtidIntervalSkipList* intervals;
    struct uuidSet *next;
} uuidSet;

typedef struct uuidSetPurged {
    size_t node_count;
    gno_t gno_count;
    gno_t start;
    gno_t end;
} uuidSetPurged;

typedef struct gtidSet {
    struct uuidSet* header;
    struct uuidSet* tail;
} gtidSet;

typedef struct gtidStat {
   size_t used_memory;
   size_t uuid_count;
   size_t gap_count;
   gno_t gno_count;
} gtidStat;

uuidSet *uuidSetNew(const char* uuid, size_t uuid_len);
void uuidSetFree(uuidSet* uuid_set);
uuidSet *uuidSetDup(uuidSet* uuid_set);
ssize_t uuidSetEncode(char *buf, size_t maxlen, uuidSet* uuid_set);
uuidSet *uuidSetDecode(char* repr, int len);
gno_t uuidSetAdd(uuidSet* uuid_set, gno_t start, gno_t end);
gno_t uuidSetRemove(uuidSet* uuid_set, gno_t start, gno_t end);
gno_t uuidSetRaise(uuidSet* uuid_set, gno_t gno);
gno_t uuidSetMerge(uuidSet* uuid_set, uuidSet* other);
gno_t uuidSetDiff(uuidSet* uuid_set, uuidSet* other);
gno_t uuidSetNext(uuidSet* uuid_set, int update);
gno_t uuidSetCurrent(uuidSet* uuid_set);
gno_t uuidSetCount(uuidSet* uuid_set);
int uuidSetContains(uuidSet* uuid_set, gno_t gno);
size_t uuidSetEstimatedEncodeBufferSize(uuidSet* uuid_set);
size_t uuidSetPurge(uuidSet *uuid_set, size_t memory_limit, uuidSetPurged *purged);
void uuidSetGetStat(uuidSet *uuid_set, gtidStat *stat);

gtidSet* gtidSetNew();
void gtidSetFree(gtidSet* gtid_set);
gtidSet* gtidSetDup(gtidSet *gtid_set);
gtidSet *gtidSetDecode(char* repr, size_t len);
ssize_t gtidSetEncode(char *buf, size_t maxlen, gtidSet* gtid_set);
gno_t gtidSetAddRange(gtidSet* gtid_set, const char* uuid, size_t uuid_len, gno_t start, gno_t end);
#define gtidSetAdd(gtid_set, uuid, uuid_len, gno) gtidSetAddRange(gtid_set, uuid, uuid_len, gno, gno)
gno_t gtidSetRaise(gtidSet* gtid_set, const char* uuid, size_t uuid_len, gno_t gno);
gno_t gtidSetMerge(gtidSet* gtid_set, gtidSet* other);
gno_t gtidSetDiff(gtidSet* gtid_set, gtidSet* other);
gno_t gtidSetAppend(gtidSet *gtid_set, uuidSet *uuid_set);
uuidSet* gtidSetFind(gtidSet* gtid_set,const char* uuid, size_t len);
int gtidSetRemove(gtidSet* gtid_set, const char *uuid, size_t uuid_len);
gno_t gtidSetCount(gtidSet *gtid_set);
size_t gtidSetEstimatedEncodeBufferSize(gtidSet* gtid_set);
void gtidSetGetStat(gtidSet *gtid_set, gtidStat *stat);

ssize_t uuidGnoEncode(char *buf, size_t maxlen, const char *uuid, size_t uuid_len, gno_t gno);
char* uuidGnoDecode(char* src, size_t src_len, long long* gno, int* uuid_len);

const char *gtidAllocatorName();

typedef uint16_t segoff_t;

#define SEGOFF_MAX UINT16_MAX
#define SEGMENT_SIZE (SEGOFF_MAX+1)

/* assuming every command is 1024 byte */
#define GTID_SEGMENT_NGNO_DEFAULT (SEGMENT_SIZE/1024)

typedef struct gtidSegment {
    struct gtidSegment *next;
    struct gtidSegment *prev;
    char *uuid;
    size_t uuid_len;
    long long base_offset;
    gno_t base_gno;
    size_t tgno; /* trimmed gno count */
    size_t ngno; /* gno count */
    size_t capacity; /* gno capacity */
    segoff_t *deltas;
} gtidSegment;

gtidSegment *gtidSegmentNew();
void gtidSegmentReset(gtidSegment *seg, const char *uuid, size_t uuid_len, gno_t base_gno, long long base_offset);
void gtidSegmentAppend(gtidSegment *seg, long long offset);
void gtidSegmentFree(gtidSegment *seg);

typedef struct gtidSeq {
    size_t segment_size;
    size_t nsegment;
    size_t nfreeseg;
    size_t nsegment_deltas; /* deltas count of occupied segment list */
    size_t nfreeseg_deltas; /* deltas count of vacant segment list */
    struct gtidSegment *firstseg; /* head of occupied segment list */
    struct gtidSegment *lastseg; /* tail of occupied segment list */
    struct gtidSegment *freeseg; /* head of vacant segment list */
} gtidSeq;

typedef struct gtidSeqStat {
   size_t used_memory;
   size_t segment_memory;
   size_t freeseg_memory;
} gtidSeqStat;

gtidSeq *gtidSeqCreate();
void gtidSeqDestroy(gtidSeq *seq);
void gtidSeqAppend(gtidSeq *seq, const char *uuid, size_t uuid_len, gno_t gno, long long offset);
void gtidSeqTrim(gtidSeq *seq, long long until);
long long gtidSeqXsync(gtidSeq *seq, gtidSet *req, gtidSet **pcont);
gtidSet *gtidSeqPsync(gtidSeq *seq, long long offset);
void gtidSeqGetStat(gtidSeq *seq, gtidSeqStat *stat);

#endif  /* __REDIS_CTRIP_GTID_H */
