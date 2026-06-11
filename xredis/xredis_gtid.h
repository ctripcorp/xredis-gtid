#ifndef  __XREDIS_GTID_H__
#define  __XREDIS_GTID_H__

/* Copyright (c) 2025, ctrip.com
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

#include <string.h>
#include "sds.h"
#include "adlist.h"
#include "connection.h"
#include "gtid.h"
#include "dict.h"

typedef struct client client;
typedef struct redisObject robj;
typedef struct _rio rio;
#include "redis_gtid.h"

/* Misc */
int isGtidExecCommand(client *c);
sds gtidSetDump(gtidSet *gtid_set);
sds gtidSetQuoteIfEmpty(sds gtid_repr);
gtidSet *serverGtidSetGet(char *log_prefix);
int serverGtidSetContains(char *uuid, size_t uuid_len, gno_t gno);
void serverGtidSetResetExecuted(gtidSet *gtid_executed);
void serverGtidSetResetLost(gtidSet *gtid_lost);
void serverGtidSetAddLost(gtidSet *delta_lost);
void serverGtidSetRemoveLost(gtidSet *delta_lost);
const char *getMasterUuid(size_t *puuid_len);
void setMasterUuid(const char *master_uuid, size_t master_uuid_len);
void clearMasterUuid();

/* Repl stream */
#define CONFIG_RUN_ID_SIZE 40

#define REPL_MODE_UNSET -1
#define REPL_MODE_PSYNC 0
#define REPL_MODE_XSYNC 1
#define REPL_MODE_TYPES  2

typedef struct replMode {
  long long from;
  int mode;
  union {
    struct {
      char replid[CONFIG_RUN_ID_SIZE+1];
      char replid2[CONFIG_RUN_ID_SIZE+1];
      long long second_replid_offset;
    } psync;
    struct {
      char replid[CONFIG_RUN_ID_SIZE+1];
      long long gtid_reploff_delta;
    } xsync;
  };
} replMode;

static inline const char *replModeName(int mode) {
  const char *name = "?";
  const char *modes[] = {"?","psync","xsync"};
  if (mode >= -1 && mode < REPL_MODE_TYPES)
    name = modes[mode+1];
  return name;
}

static inline void replModeInit(replMode *repl_mode) {
  memset(repl_mode,0,sizeof(struct replMode));
  repl_mode->from = -1;
  repl_mode->mode = REPL_MODE_UNSET;
}

const char *serverReplModeGetCurDetail();
const char *serverReplModeGetPrevDetail();
const char *serverReplModeGetCurReplIdOff(long long offset, long long *reploff);
const char *serverReplModeGetPrevReplIdOff(long long offset, long long *reploff);

long long ctrip_getMasterReploff();
void ctrip_setMasterReploff(long long reploff);


#define LOCATE_TYPE_UNSET     0
#define LOCATE_TYPE_INVALID   1
#define LOCATE_TYPE_PREV      2
#define LOCATE_TYPE_SWITCH    3
#define LOCATE_TYPE_CUR       4

typedef struct syncLocateResult {
  int locate_mode;
  int locate_type;
  union {
    struct {
      sds msg;
    } i; /* invalid */
    struct {
      long long limit;
    } p; /* prev */
  };
} syncLocateResult;

void syncLocateResultInit(syncLocateResult *slr);
void syncLocateResultDeinit(syncLocateResult *slr);
void locateServerReplMode(int request_mode, long long psync_offset, syncLocateResult *slr);


#define GTID_COMMAN_ARGC 3

typedef struct propagateArgs {
    struct redisCommand *orig_cmd;
    robj **orig_argv;
    int orig_argc;
    int orig_dbid;
    /* repl backlog (might be rewritten) */
    struct redisCommand *cmd;
    int argc;
    robj **argv; /* argv[1~2] owned if argv was rewritten */
    /* gtid_seq index */
    char *uuid; /* ref to server.current_uuid or argv[1] */
    size_t uuid_len;
    gno_t gno;
    long long offset;
} propagateArgs;

void propagateArgsInit(propagateArgs *pargs, struct redisCommand *cmd, int dbid, robj **argv, int argc);
void propagateArgsPrepareToFeed(propagateArgs *pargs);
void propagateArgsDeinit(propagateArgs *pargs);

void ctrip_createReplicationBacklog(void);
void ctrip_resizeReplicationBacklog(long long newsize);
void ctrip_freeReplicationBacklog(void);
void ctrip_replicationFeedSlaves(list *slaves, int dictid, robj **argv, int argc, const char *uuid, size_t uuid_len, gno_t gno, long long offset);
void ctrip_replicationFeedSlavesFromMasterStream(list *slaves, char *buf, size_t buflen, const char *uuid, size_t uuid_len, gno_t gno, long long offset);
void ctrip_feedAppendOnlyFile(struct redisCommand *cmd, int dictid, robj **argv, int argc);

typedef struct gtidInitialInfo {
  gtidSet *gtid_lost;
  sds master_uuid;
  sds replid;
  long long reploff;
} gtidInitialInfo;

void gtidInitialInfoInit(gtidInitialInfo *info);
void gtidInitialInfoSetup(gtidInitialInfo *info, gtidSet *gtid_lost, sds master_uuid, sds replid, long long reploff);

#define RS_UPDATE_NOP  0
#define RS_UPDATE_DOWN (1<<0)

void serverReplStreamMasterLinkBroken();
void serverReplStreamResurrectCreate(connection *conn, int dbid, const char *replid, long long reploff);
void serverReplStreamUpdateXsync(gtidSet *gtid_lost, client *trigger_slave, sds master_uuid, sds replid, long long reploff);
void serverReplStreamSwitch2Xsync(sds replid, long long reploff, sds master_uuid, gtidSet *gtid_executed, gtidSet *gtid_lost, int flags, const char *log_prefix);
void serverReplStreamSwitch2Psync(const char *replid, long long reploff, int flags, const char *log_prefix);
void serverReplStreamSwitchIfNeeded(int to_mode, int flags, const char *log_prefix);
void serverReplStreamReset2Xsync(sds replid, long long reploff, int repl_stream_db, sds master_uuid, gtidSet *gtid_executed, gtidSet *gtid_lost, int flags, const char *log_prefix);
void serverReplStreamReset2Psync(const char *replid, long long reploff, int flags, const char *log_prefix);

/* Rdb */
typedef struct rdbSaveInfo rdbSaveInfo;

typedef struct rdbSaveInfoGtid {
  int repl_mode;
  gtidSet *gtid_executed;
  gtidSet *gtid_lost;
} rdbSaveInfoGtid;

rdbSaveInfoGtid *rdbSaveInfoGtidCreate();
void rdbSaveInfoGtidDestroy(rdbSaveInfoGtid *gtid_rsi);
int rdbSaveInfoAuxFieldsGtid(rio* rdb, rdbSaveInfo *rsi);
int loadInfoAuxFieldsGtid(robj* key, robj* val, rdbSaveInfo *rsi);

/* Replication (Xsync & Psync) */
#define PSYNC_BY_REDIS -1
#define PSYNC_WRITE_ERROR 0
#define PSYNC_WAIT_REPLY 1
#define PSYNC_CONTINUE 2
#define PSYNC_FULLRESYNC 3
#define PSYNC_NOT_SUPPORTED 4
#define PSYNC_TRY_LATER 5
#define PSYNC_FULLRESYNC_RDBCHANNEL 6

#define GTID_SHIFT_REPL_STREAM_DISCARD_CACHED_MASTER    (1<<0)
#define GTID_SHIFT_REPL_STREAM_NOTIFY_SLAVES            (1<<1)
#define GTID_SHIFT_REPL_STREAM_FULL                     (GTID_SHIFT_REPL_STREAM_DISCARD_CACHED_MASTER|GTID_SHIFT_REPL_STREAM_NOTIFY_SLAVES)

#define GTID_XSYNC_UUID_INTERESTED_DEFAULT      "*"
#define GTID_XSYNC_UUID_INTERESTED_FULLRESYNC   "?"

#define GTID_SYNC_PSYNC_FULLRESYNC  0
#define GTID_SYNC_PSYNC_CONTINUE    1
#define GTID_SYNC_PSYNC_XFULLRESYNC 2
#define GTID_SYNC_PSYNC_XCONTINUE   3
#define GTID_SYNC_XSYNC_FULLRESYNC  4
#define GTID_SYNC_XSYNC_CONTINUE    5
#define GTID_SYNC_XSYNC_XFULLRESYNC 6
#define GTID_SYNC_XSYNC_XCONTINUE   7
#define GTID_SYNC_TYPES             8

static inline const char *gtidSyncTypeName(int type) {
  const char *name = "?";
  const char *types[] = {"psync_fullresync","psync_continue","psync_xfullresync","psync_xcontinue","xsync_fullresync","xsync_continue","xsync_xfullresync","xsync_xcontinue"};
  if (type >= 0 && type < GTID_SYNC_TYPES)
    name = types[type];
  return name;
}

void xsyncUuidInterestedInit(void);
void forceXsyncFullResync(void);
void xsyncReplicationCron(void);
void resetServerReplMode(int mode, const char *log_prefix);
void shiftServerReplMode(int mode, const char *log_prefix);
sds genGtidInfoString(sds info);
void gtidCommand(client *c);
void gtidxCommand(client *c);
char *ctrip_receiveSynchronousResponse(connection *conn);
int ctrip_replicationSetupSlaveForFullResync(client *slave, long long offset);
int ctrip_masterTryPartialResynchronization(client *c, long long offset);
int ctrip_addReplyReplicationBacklog(client *c, long long offset, long long *added);
int ctrip_slaveTryPartialResynchronizationWrite(connection *conn);
int ctrip_slaveTryPartialResynchronizationRead(connection *conn, sds reply);
void ctrip_afterErrorReply(client *c, const char *s, size_t len, int flags);
sds sendXsyncCommand(connection *conn);

/* Expose functions that used by gtid */
void createReplicationBacklog(void);
char *sendCommand(connection *conn, ...);
int cancelReplicationHandshake(int reconnect);
void replicationDiscardCachedMaster(void);
void replicationCreateMasterClient(connection *conn, int dbid);
void aofRewriteBufferAppend(unsigned char *s, unsigned long len);
sds catAppendOnlyGenericCommand(sds dst, int argc, robj **argv);
long long addReplyReplicationBacklog(client *c, long long offset);
ssize_t rdbSaveAuxField(rio *rdb, void *key, size_t keylen, void *val, size_t vallen);

#define OBJ_UNKNOWN 255

#include "xredis_gtid_cmdparse.h"

/* ================================================================
 * gapLog functions and structs
 * ================================================================ */
typedef struct gtidGaplogKey {
  unsigned long long dbid:4;      /* max 16 db */
  unsigned long long key_type:4;  /* OBJ_STRING/OBJ_LIST/OBJ_SET/OBJ_ZSET/OBJ_HASH */
  unsigned long long subkeys_count:56;
  sds key;                        /* key (sdsdup ) */
  sds* subkeys;                   /* subkeys (sdsdup ) */
} gtidGaplogKey;

typedef struct gtidGaplogKeys {
    gtidGaplogKey** keys;
    size_t size;
} gtidGaplogKeys;

#define GTID_GAPLOG_MAX_KEYS_BUFFER 256
#define GTID_GAPLOG_HISTORY_MAX_COUNT 100
typedef struct gtidGaplogKeysBuilder {
  gtidGaplogKey* cache[GTID_GAPLOG_MAX_KEYS_BUFFER];
  gtidGaplogKey** keys_infos;
  int numkeys;
  int size;
} gtidGaplogKeysBuilder;
#define GTID_GAPLOG_KEYS_BUILDER_INIT {{0}, NULL, 0, GTID_GAPLOG_MAX_KEYS_BUFFER}
gtidGaplogKeys* gtidGaplogKeysBuild(gtidGaplogKeysBuilder* builder);
gtidGaplogKey** gtidGaplogKeysPrepareBuilder(gtidGaplogKeysBuilder* builder, int add_numkeys);
void gtidGaplogDeinitKeysBuilder(gtidGaplogKeysBuilder* builder);
void gtidGaplogKeysBuilderAddFromCmd(gtidGaplogKeysBuilder* builder, int dbid, robj **args, int argc);

int cmdGetKeyType(struct redisCommand *cmd);

typedef struct gtidGaplog {
  dict* data;           //dict<uuid, skiplist<gtidGaplogKey>>
  list* history;   //list<uuidSet>
  size_t size;  
} gtidGaplog;

gtidGaplog* gtidGaplogNew();
void gtidGaplogReset(gtidGaplog* gtid_gap_log);
void gtidGaplogRelease(gtidGaplog* gaplog);
int gtidGaplogTrim(gtidGaplog* log ,size_t size);

typedef struct gtidGaplogDataIterator {
  skiplistIterator sl_iter;
} gtidGaplogDataIterator;
void gtidGaplogDataInitIterator(gtidGaplogDataIterator *iter, skiplist *sl, gno_t start_gno);
void gtidGaplogDeinitDataIterator(gtidGaplogDataIterator *iter);
void gtidGaplogDataIteratorSeek(gtidGaplogDataIterator *iter, gno_t gno);
gno_t gtidGaplogDataGetGno(gtidGaplogDataIterator* iter);
gtidGaplogKeys* gtidGaplogDataNext(gtidGaplogDataIterator* iterator);


typedef struct gtidGaplogHistoryIterator {
    list* history;                /* gaplog->history list, for re-seek from head */
    listNode* list_node;          /* current uuidSet node in history list */
    gtidIntervalNode* interval_node;  /* current interval node within uuidSet */
    gno_t next_gno;                   /* next gno to return */

} gtidGaplogHistoryIterator;
void gtidGaplogInitHistoryIterator(gtidGaplogHistoryIterator* iter,
                                    gtidGaplog* gaplog, long long index);
gno_t gtidGaplogHistoryNext(gtidGaplogHistoryIterator* iter,
                             const char** uuid, size_t* uuid_len);
void gtidGaplogHistoryIteratorSeek(gtidGaplogHistoryIterator* iter, long long index);
void gtidGaplogDeinitHistoryIterator(gtidGaplogHistoryIterator* iter);

/* readBacklogIterator: iterate commands from replication backlog with querybuf reuse.
 * Use Init/SeekTo/ParseNext/Deinit. backlog == -1 means "not seeked yet".
 * Full struct definition is in xredis_gtid_repl.c (where `client` is complete). */
typedef struct readBacklogIterator readBacklogIterator;

void readBacklogIteratorInit(readBacklogIterator *it);
void readBacklogIteratorDeinit(readBacklogIterator *it);
void readBacklogIteratorSeekTo(readBacklogIterator *it, long long offset);
ssize_t readBacklogIteratorParseNext(readBacklogIterator *it,
                                      robj ***out_argv, int *out_argc);
void parseMultiCommand(gtidGaplogKeysBuilder *build,
                       readBacklogIterator *it,
                       long long select_dbid);
int parseGtidCommand(gtidGaplogKeysBuilder *builder, robj **argv, int argc);
void addReplyGtidGaplogKeys(client* c, gtidGaplogKeys* keys);
void gtidGaplogKeysRelease(void* keys);
gtidGaplogKey* gtidGaplogKeyNew(int dbid, int type, sds key, sds* subkeys, int subkeys_count);
void gtidGaplogKeyRelease(gtidGaplogKey* key);
int processMultibulkBuffer(client* c);


/* gtid test */
int gtidTest(int argc, char **argv, int accurate);

#endif
