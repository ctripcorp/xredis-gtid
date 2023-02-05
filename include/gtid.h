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
    struct uuidSet* header;
    struct uuidSet* tail;
} gtidSet;

uuidSet *uuidSetNew(const char* uuid, size_t uuid_len);
void uuidSetFree(uuidSet* uuid_set);
uuidSet *uuidSetDup(uuidSet* uuid_set);
ssize_t uuidSetEncode(char *buf, size_t maxlen, uuidSet* uuid_set);
uuidSet *uuidSetDecode(char* repr, int len);
gno_t uuidSetAdd(uuidSet* uuid_set, gno_t start, gno_t end);
gno_t uuidSetRaise(uuidSet* uuid_set, gno_t gno);
gno_t uuidSetMerge(uuidSet* uuid_set, uuidSet* other);
gno_t uuidSetNext(uuidSet* uuid_set, int update);
int uuidSetContains(uuidSet* uuid_set, gno_t gno);
size_t uuidSetEstimatedEncodeBufferSize(uuidSet* uuid_set);

gtidSet* gtidSetNew();
void gtidSetFree(gtidSet* gtid_set);
gtidSet *gtidSetDecode(char* repr, size_t len);
ssize_t gtidSetEncode(char *buf, size_t maxlen, gtidSet* gtid_set);
gno_t gtidSetAdd(gtidSet* gtid_set, const char* uuid, size_t uuid_len, gno_t gno);
gno_t gtidSetRaise(gtidSet* gtid_set, const char* uuid, size_t uuid_len, gno_t gno);
gno_t gtidSetMerge(gtidSet* gtid_set, gtidSet* other);
gno_t gtidSetAppend(gtidSet *gtid_set, uuidSet *uuid_set);
uuidSet* gtidSetFind(gtidSet* gtid_set,const char* uuid, size_t len);
size_t gtidSetEstimatedEncodeBufferSize(gtidSet* gtid_set);

ssize_t uuidGnoEncode(char *buf, size_t maxlen, const char *uuid, size_t uuid_len, gno_t gno);
char* uuidGnoDecode(char* src, size_t src_len, long long* gno, int* uuid_len);

#endif  /* __REDIS_CTRIP_GTID_H */
