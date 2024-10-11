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
#include <assert.h>

#define GTID_GNO_INITIAL        1

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

typedef struct gtidSet {
    /* next gno for current if > 0 */
    gno_t curnext;
    struct uuidSet *current;
    /* cache last accessed uuidset.
     * last accessed uuid are most likey to be accessed again */
    struct uuidSet *cached;
    struct uuidSet* header;
    struct uuidSet* tail;
} gtidSet;

typedef struct gtidStat {
   size_t used_memory;
   size_t uuid_count;
   size_t gap_count;
   gno_t gno_count;
} gtidStat;

const char *gtidAllocatorName();

ssize_t uuidGnoEncode(char *buf, size_t maxlen, const char *uuid, size_t uuid_len, gno_t gno);
char* uuidGnoDecode(char* src, size_t src_len, long long* gno, size_t* uuid_len);

uuidSet *uuidSetNew(const char* uuid, size_t uuid_len);
void uuidSetFree(uuidSet* uuid_set);
uuidSet *uuidSetDup(uuidSet* uuid_set);
ssize_t uuidSetEncode(char *buf, size_t maxlen, uuidSet* uuid_set);
uuidSet *uuidSetDecode(char* repr, int len);
gno_t uuidSetAdd(uuidSet* uuid_set, gno_t start, gno_t end);
gno_t uuidSetRemove(uuidSet* uuid_set, gno_t start, gno_t end);
gno_t uuidSetMerge(uuidSet* uuid_set, uuidSet* other);
gno_t uuidSetDiff(uuidSet* uuid_set, uuidSet* other);
gno_t uuidSetNext(uuidSet* uuid_set, int update);
gno_t uuidSetCount(uuidSet* uuid_set);
int uuidSetContains(uuidSet* uuid_set, gno_t gno);
size_t uuidSetEstimatedEncodeBufferSize(uuidSet* uuid_set);
void uuidSetGetStat(uuidSet *uuid_set, gtidStat *stat);

gtidSet* gtidSetNew();
void gtidSetFree(gtidSet* gtid_set);
gtidSet* gtidSetDup(gtidSet *gtid_set);
gtidSet *gtidSetDecode(char* repr, size_t len);
ssize_t gtidSetEncode(char* buf, size_t maxlen, gtidSet* gtid_set);
gno_t gtidSetAdd(gtidSet* gtid_set, const char* uuid, size_t uuid_len, gno_t start, gno_t end);
gno_t gtidSetRemove(gtidSet *gtid_set, const char* uuid, size_t uuid_len, gno_t start, gno_t end);
gno_t gtidSetMerge(gtidSet* gtid_set, gtidSet* other);
gno_t gtidSetDiff(gtidSet* gtid_set, gtidSet* other);
gno_t gtidSetNext(gtidSet* gtid_set, const char* uuid, size_t uuid_len, int upate);
gno_t gtidSetCount(gtidSet *gtid_set);
int gtidSetEqual(gtidSet *set1, gtidSet *set2);
int gtidSetContains(gtidSet* gtid_set, const char* uuid, size_t uuid_len, gno_t gno);
size_t gtidSetEstimatedEncodeBufferSize(gtidSet* gtid_set);
void gtidSetGetStat(gtidSet *gtid_set, gtidStat *stat);
uuidSet* gtidSetFind(gtidSet* gtid_set, const char* uuid, size_t uuid_len);
int gtidSetRelated(gtidSet *set1, gtidSet *set2);

/* Cache current uuid set to skip uuid compare. Note that it would crash
 * if current uuid set not cached or removed. */
void gtidSetCurrentUuidSetUpdate(gtidSet *gtid_set, const char *uuid, size_t uuid_len);
static inline void gtidSetCurrentUuidSetSetNextGno(gtidSet *gtid_set, gno_t curnext) {
  assert(gtid_set->current);
  assert(uuidSetNext(gtid_set->current,0) <= curnext);
  gtid_set->curnext = curnext;
  gtid_set->cached = gtid_set->current;
}
static inline gno_t gtidSetCurrentUuidSetNext(gtidSet *gtid_set, int update) {
  gtid_set->cached = gtid_set->current;
  if (gtid_set->curnext == 0) {
    return uuidSetNext(gtid_set->current, update);
  } else {
    gno_t curnext = gtid_set->curnext;
    if (update) {
      uuidSetAdd(gtid_set->current,curnext,curnext);
      gtid_set->curnext = 0;
    }
    return curnext;
  }
}

typedef uint16_t segoff_t;

#define SEGOFF_MAX UINT16_MAX
#define SEGMENT_SIZE (SEGOFF_MAX+1)

#define GTID_ESTIMATED_CMD_SIZE 1024
#define GTID_SEGMENT_NGNO_DEFAULT (SEGMENT_SIZE/GTID_ESTIMATED_CMD_SIZE)

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
    size_t nsegment_deltas;
    size_t nfreeseg_deltas;
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
size_t gtidSeqEstimatedEncodeBufferSize(gtidSeq* seq);
ssize_t gtidSeqEncode(char *buf, size_t maxlen, gtidSeq* seq);
long long gtidSeqXsync(gtidSeq *seq, gtidSet *req, gtidSet **pcont);
gtidSet *gtidSeqPsync(gtidSeq *seq, long long offset);
void gtidSeqGetStat(gtidSeq *seq, gtidSeqStat *stat);

#endif  /* __REDIS_CTRIP_GTID_H */
