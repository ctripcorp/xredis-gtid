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

typedef struct client client;
typedef struct redisObject robj;
typedef struct _rio rio;

/* Misc */
int isGtidExecCommand(client *c);
sds gtidSetDump(gtidSet *gtid_set);
gtidSet *serverGtidSetGet(char *log_prefix);
int serverGtidSetContains(char *uuid, size_t uuid_len, gno_t gno);
void serverGtidSetResetExecuted(gtidSet *gtid_executed);
void serverGtidSetResetLost(gtidSet *gtid_lost);
void serverGtidSetAddLost(gtidSet *delta_lost);
void serverGtidSetRemoveLost(gtidSet *delta_lost);
void serverGtidSetAddExecuted(gtidSet *delta_executed);
void serverGtidSetRemoveExecuted(gtidSet *delta_executed);
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
void serverReplStreamSwitch2Xsync(sds replid, long long reploff, sds master_uuid, int flags, const char *log_prefix);
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
int ctrip_masterTryPartialResynchronization(client *c);
int ctrip_addReplyReplicationBacklog(client *c, long long offset, long long *added);
int ctrip_slaveTryPartialResynchronizationWrite(connection *conn);
int ctrip_slaveTryPartialResynchronizationRead(connection *conn, sds reply);
void ctrip_afterErrorReply(client *c, const char *s, size_t len);


/* Expose functions that used by gtid */
void createReplicationBacklog(void);
char *sendCommand(connection *conn, ...);
int cancelReplicationHandshake(int reconnect);
void replicationDiscardCachedMaster(void);
void replicationCreateMasterClient(connection *conn, int dbid);
void aofRewriteBufferAppend(unsigned char *s, unsigned long len);
int masterTryPartialResynchronization(client *c);
sds catAppendOnlyGenericCommand(sds dst, int argc, robj **argv);
long long addReplyReplicationBacklog(client *c, long long offset);
void afterErrorReply(client *c, const char *s, size_t len);
ssize_t rdbSaveAuxField(rio *rdb, void *key, size_t keylen, void *val, size_t vallen);

int gtidTest(int argc, char **argv, int accurate);

#endif
