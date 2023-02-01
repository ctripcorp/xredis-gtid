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


#include <stdio.h>

 /* start from 1 */
typedef long long int gno_t;

typedef struct gtidInterval {
    gno_t start;
    gno_t end;
    struct gtidInterval *next;
} gtidInterval;

typedef struct uuidSet {
    struct gtidInterval* intervals;
    struct uuidSet *next;
    char* sid;
    size_t sid_len;
} uuidSet;

typedef struct gtidSet {
    struct uuidSet* uuid_sets;
    struct uuidSet* tail;
} gtidSet;

gtidInterval *gtidIntervalNew(gno_t gno);
gtidInterval *gtidIntervalDup(gtidInterval* gtid_interval);
void gtidIntervalFree(gtidInterval* interval);
gtidInterval *gtidIntervalNewRange(gno_t start, gno_t end);
gtidInterval *gtidIntervalDecode(char* repr, size_t len);
size_t gtidIntervalEncode(gtidInterval* interval, char* buf);

uuidSet *uuidSetNew(const char* sid, size_t sid_len, gno_t gno);
void uuidSetFree(uuidSet* uuid_set);
uuidSet *uuidSetDup(uuidSet* uuid_set);
uuidSet *uuidSetNewRange(const char* sid, size_t sid_len, gno_t start, gno_t end);
uuidSet *uuidSetDecode(char* repr, int len);

size_t uuidSetEstimatedEncodeBufferSize(uuidSet* uuid_set);
size_t uuidSetEncode(uuidSet* uuid_set, char* buf);
int uuidSetAdd(uuidSet* uuid_set, gno_t gno);
void uuidSetRaise(uuidSet* uuid_set, gno_t gno);
int uuidSetContains(uuidSet* uuid_set, gno_t gno);
gno_t uuidSetNext(uuidSet* uuid_set, int update);
size_t uuidSetNextEncode(uuidSet* uuid_set, int update, char* buf);
int uuidSetAppendUuidSet(uuidSet* uuid_set, uuidSet* other);
int uuidSetAddGtidInterval(uuidSet* uuid_set, gtidInterval* interval);

char* uuidDecode(char* src, size_t src_len, long long* gno, int* sid_len);
uuidSet* gtidSetFindUuidSet(gtidSet* gtid_set,const char* sid, size_t len);

gtidSet* gtidSetNew();
void gtidSetFree(gtidSet*);
gtidSet *gtidSetDecode(char* repr, size_t len);
size_t gtidSetEstimatedEncodeBufferSize(gtidSet* gtid_set);
size_t gtidSetEncode(gtidSet* gtid_set, char* buf);
int gtidSetAdd(gtidSet* gtid_set, const char* sid, size_t sid_len, gno_t gno);
void gtidSetRaise(gtidSet* gtid_set, const char* sid, size_t sid_len, gno_t gno);
void gtidSetAppendGtidSet(gtidSet* gtid_set, gtidSet* other);
#endif  /* __REDIS_CTRIP_GTID_H */
