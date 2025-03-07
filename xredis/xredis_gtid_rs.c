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

#include "server.h"
#include <gtid.h>
#include <ctype.h>

void propagateArgsInit(propagateArgs *pargs, struct redisCommand *cmd,
        int dbid, robj **argv, int argc) {
    pargs->orig_cmd = cmd;
    pargs->orig_argv = argv;
    pargs->orig_argc = argc;
    pargs->orig_dbid = dbid;
}

/* Prepare to feed:
 * 1. rewrite to gtid... if needed: set k v  -> gtid {gtid_repr} {dbid} set k v
 * 2. prepare info to create gtid_seq index */
void propagateArgsPrepareToFeed(propagateArgs *pargs) {
    gno_t gno = 0;
    long long offset;
    size_t bufmaxlen, buflen, uuid_len = server.uuid_len;
    int argc, dbid;
    robj **argv;
    char *buf, *uuid = server.uuid;
    sds gtid_repr;
    struct redisCommand *cmd;

    if (server.gtid_dbid_at_multi >= 0) {
        dbid = server.gtid_dbid_at_multi;
        offset = server.gtid_offset_at_multi;

        /* afterPropagateExec could be called before calling propagate()
         * for exec, so we clear gtid_xxx_at_multi here. */
        if (pargs->orig_cmd == server.execCommand ||
                (pargs->orig_cmd->proc == gtidCommand &&
                 strcasecmp(pargs->orig_argv[3]->ptr, "exec") == 0)) {
            server.gtid_dbid_at_multi = -1;
            server.gtid_offset_at_multi = -1;
        }
    } else {
        dbid = pargs->orig_dbid;
        offset = server.master_repl_offset+1;
    }

    if (server.masterhost == NULL &&
#ifdef ENABLE_SWAP
            server.swap_draining_master == NULL &&
#endif
            pargs->orig_cmd->proc == gtidCommand) {
        gtid_repr = pargs->orig_argv[1]->ptr;
        uuid = uuidGnoDecode(gtid_repr,sdslen(gtid_repr),&gno,&uuid_len);
    }

    /* Rewrite args to gtid... if needed */
    if (server.masterhost != NULL ||
#ifdef ENABLE_SWAP
            server.swap_draining_master != NULL ||
#endif
            !server.gtid_enabled ||
            pargs->orig_cmd->proc == gtidCommand ||
            pargs->orig_cmd->proc == publishCommand ||
            (server.propagate_in_transaction &&
             pargs->orig_cmd != server.execCommand)) {
        cmd = pargs->orig_cmd;
        argc = pargs->orig_argc;
        argv = pargs->orig_argv;
    } else {
        gno = gtidSetCurrentUuidSetNext(server.gtid_executed,1);

        bufmaxlen = uuid_len+1+21/* GNO_REPR_MAX_LEN */;
        buf = zmalloc(bufmaxlen);
        buflen = uuidGnoEncode(buf, bufmaxlen, uuid, uuid_len, gno);
        gtid_repr = sdsnewlen(buf, buflen);
        zfree(buf);

        cmd = server.gtidCommand;
        argc = pargs->orig_argc+3;
        argv = zmalloc(argc*sizeof(robj*));
        argv[0] = shared.gtid;
        argv[1] = createObject(OBJ_STRING, gtid_repr);
        argv[2] = createObject(OBJ_STRING, sdsfromlonglong(dbid));
        for(int i = 0; i < pargs->orig_argc; i++) {
            argv[i+3] = pargs->orig_argv[i];
        }
    }

    pargs->cmd = cmd;
    pargs->argc = argc;
    pargs->argv = argv;
    pargs->uuid = uuid;
    pargs->uuid_len = uuid_len;
    pargs->gno = gno;
    pargs->offset = offset;
}

void propagateArgsDeinit(propagateArgs *pargs) {
    if (pargs->orig_argv == pargs->argv) return;
    decrRefCount(pargs->argv[1]);
    decrRefCount(pargs->argv[2]);
    zfree(pargs->argv);
    pargs->orig_argv = pargs->argv = NULL;
}

/* Server repl mode:
 * - repl_mode: current repl mode(Note that xsync/psync detail not set)
 * - prev_repl_mode: previous repl mode, contains xsync/psync detail. */
sds dumpServerReplMode() {
    return sdscatprintf(sdsempty(),"{(%s:%lld:%s),(%s:%lld:%s)}",
            replModeName(server.repl_mode->mode),
            server.repl_mode->from,
            serverReplModeGetCurDetail(),
            replModeName(server.prev_repl_mode->mode),
            server.prev_repl_mode->from,
            serverReplModeGetPrevDetail());
}

void resetServerReplMode(int mode, const char *log_prefix) {
    sds prev, cur;
    prev = dumpServerReplMode();
    replModeInit(server.prev_repl_mode);
    server.repl_mode->mode = mode;
    server.repl_mode->from = server.master_repl_offset+1;
    cur = dumpServerReplMode();
    if (log_prefix) {
        serverLog(LL_NOTICE,"[gtid] reset repl mode to %s: %s => %s (%s)",
                replModeName(mode), prev, cur, log_prefix);
    }
    sdsfree(prev), sdsfree(cur);
}

/* Must reset server repl mode before shift. */
void shiftServerReplMode(int mode, const char *log_prefix) {
    sds prev, cur;
    replMode *prev_mode = server.prev_repl_mode, *repl_mode = server.repl_mode;
    long long from = server.master_repl_offset+1;
    serverAssert(repl_mode->mode != REPL_MODE_UNSET && repl_mode->mode != mode);
    prev = dumpServerReplMode();
    if (repl_mode->from != from) {
        prev_mode->from = repl_mode->from;
        prev_mode->mode = repl_mode->mode;
        if (mode == REPL_MODE_PSYNC) {
            memcpy(prev_mode->xsync.replid,server.replid,sizeof(server.replid));
            prev_mode->xsync.gtid_reploff_delta = server.gtid_reploff_delta;
        } else {
            memcpy(prev_mode->psync.replid,server.replid,sizeof(server.replid));
            memcpy(prev_mode->psync.replid2,server.replid2,sizeof(server.replid2));
            prev_mode->psync.second_replid_offset = server.second_replid_offset;
        }
    }
    repl_mode->from = from;
    repl_mode->mode = mode;

    cur = dumpServerReplMode();
    serverLog(LL_NOTICE,"[gtid] shift repl mode to %s: %s => %s (%s)",
            replModeName(mode), prev, cur, log_prefix);
    sdsfree(prev), sdsfree(cur);
}

/* Replid and offset of current repl mode are save in server struct */
const char *serverReplModeGetCurReplIdOff(long long offset,
        long long *preploff) {
    const char *replid = NULL;
    long long reploff = -1;
    switch (server.repl_mode->mode) {
    case REPL_MODE_PSYNC:
        replid = server.replid;
        reploff = offset;
        break;
    case REPL_MODE_XSYNC:
        replid = server.replid;
        reploff = server.gtid_reploff_delta + offset;
        break;
    default:
        break;
    }
    if (preploff) *preploff = reploff;
    return replid;
}

/* Replid and offset of prev repl mode are save in prev repl mode */
const char *serverReplModeGetPrevReplIdOff(long long offset,
        long long *preploff) {
    const char *replid = NULL;
    long long reploff = -1;
    replMode *prev_mode = server.prev_repl_mode;
    switch (prev_mode->mode) {
    case REPL_MODE_PSYNC:
        replid = prev_mode->psync.replid;
        reploff = offset;
        break;
    case REPL_MODE_XSYNC:
        replid = prev_mode->xsync.replid;
        reploff = prev_mode->xsync.gtid_reploff_delta + offset;
        break;
    default:
        break;
    }
    if (preploff) *preploff = reploff;
    return replid;
}

const char *serverReplModeGetCurDetail() {
    static char detail[CONFIG_RUN_ID_SIZE*4] = {0};
    switch (server.repl_mode->mode) {
    case REPL_MODE_PSYNC:
        snprintf(detail,sizeof(detail),
                "replid=%s,offset=%lld,replid2=%s,offset2=%lld",
                server.replid,server.master_repl_offset,
                server.replid2,server.second_replid_offset);
        break;
    case REPL_MODE_XSYNC:
        snprintf(detail,sizeof(detail),
                "replid=%s,offset=%lld,delta=%lld",
                server.replid,server.master_repl_offset,
                server.gtid_reploff_delta);
        break;
    default:
        detail[0] = '\0';
        break;
    }
    return detail;
}

const char *serverReplModeGetPrevDetail() {
    static char detail[CONFIG_RUN_ID_SIZE*4] = {0};
    replMode *prev_mode = server.prev_repl_mode;
    switch (prev_mode->mode) {
    case REPL_MODE_PSYNC:
        snprintf(detail,sizeof(detail),
                "replid=%s,replid2=%s,offset2=%lld",
                prev_mode->psync.replid,prev_mode->psync.replid2,
                prev_mode->psync.second_replid_offset);
        break;
    case REPL_MODE_XSYNC:
        snprintf(detail,sizeof(detail),
                "replid=%s,delta=%lld",prev_mode->xsync.replid,
                prev_mode->xsync.gtid_reploff_delta);
        break;
    default:
        detail[0] = '\0';
        break;
    }
    return detail;
}

long long ctrip_getMasterReploff() {
    serverAssert(server.repl_mode->mode == REPL_MODE_XSYNC ||
            server.gtid_reploff_delta == 0);
    return server.master_repl_offset + server.gtid_reploff_delta;
}

void ctrip_setMasterReploff(long long reploff) {
    serverAssert(server.repl_mode->mode == REPL_MODE_XSYNC ||
            reploff == server.master_repl_offset);
    server.gtid_reploff_delta = reploff - server.master_repl_offset;
}


void syncLocateResultInit(syncLocateResult *slr) {
    memset(slr,0,sizeof(syncLocateResult));
}

void syncLocateResultDeinit(syncLocateResult *slr) {
    if (slr->locate_type == LOCATE_TYPE_INVALID) {
        sdsfree(slr->i.msg);
        slr->i.msg = NULL;
    }
}

void locateServerReplMode(int request_mode, long long psync_offset,
        syncLocateResult *slr) {
    serverAssert(server.repl_mode->mode != REPL_MODE_UNSET);
    serverAssert(request_mode == REPL_MODE_PSYNC ||
            request_mode == REPL_MODE_XSYNC);

    if (psync_offset > server.repl_mode->from) {
        if (request_mode == server.repl_mode->mode) {
            slr->locate_mode = server.repl_mode->mode;
            slr->locate_type = LOCATE_TYPE_CUR;
        } else {
            slr->locate_type = LOCATE_TYPE_INVALID;
            slr->i.msg = sdscatprintf(sdsempty(),
                    "request mode(%s) != located mode(%s)",
                    replModeName(request_mode),
                    replModeName(server.repl_mode->mode));
        }
    } else if (psync_offset == server.repl_mode->from) {
        slr->locate_mode = server.repl_mode->mode;
        if (request_mode == server.repl_mode->mode) {
            slr->locate_type = LOCATE_TYPE_CUR;
        } else if (server.prev_repl_mode->mode == REPL_MODE_UNSET) {
            slr->locate_type = LOCATE_TYPE_INVALID;
            slr->i.msg = sdsnew("prev repl mode not valid");
        } else {
            slr->locate_type = LOCATE_TYPE_SWITCH;
        }
    } else if (server.prev_repl_mode->mode == REPL_MODE_UNSET) {
        slr->locate_type = LOCATE_TYPE_INVALID;
        slr->i.msg = sdscatprintf(sdsempty(),
                "psync offset(%lld) < repl_mode.from(%lld)",
                psync_offset,server.repl_mode->from);
    } else if (psync_offset >= server.prev_repl_mode->from) {
        serverAssert(server.prev_repl_mode->from < server.repl_mode->from);
        if (request_mode == server.prev_repl_mode->mode) {
            slr->locate_mode = server.prev_repl_mode->mode;
            slr->locate_type = LOCATE_TYPE_PREV;
            slr->p.limit = server.repl_mode->from - psync_offset;
            serverAssert(slr->p.limit > 0);
        } else {
            slr->locate_type = LOCATE_TYPE_INVALID;
            slr->i.msg = sdscatprintf(sdsempty(),
                    "request mode(%s) != located mode(%s)",
                    replModeName(request_mode),
                    replModeName(server.prev_repl_mode->mode));
        }
    } else {
        slr->locate_type = LOCATE_TYPE_INVALID;
        slr->i.msg = sdscatprintf(sdsempty(),
                "psync offset(%lld) < prev_repl_mode.from(%lld)",
                psync_offset,server.prev_repl_mode->from);
    }
}

/* Backlog with gtid index */
void ctrip_createReplicationBacklog(void) {
    serverAssert(server.gtid_seq == NULL);
    createReplicationBacklog();
    server.gtid_seq = gtidSeqCreate();
}

void ctrip_resizeReplicationBacklog(long long newsize) {
    long long oldsize = server.repl_backlog_size;
    resizeReplicationBacklog(newsize);
    if (server.repl_backlog != NULL && oldsize != server.repl_backlog_size) {
        /* realloc a new gtidSeq to keep gtid_seq sync with backlog, see
         * resizeReplicationBacklog for more details. */
        gtidSeqDestroy(server.gtid_seq);
        server.gtid_seq = gtidSeqCreate();
    }
}

void ctrip_freeReplicationBacklog(void) {
    freeReplicationBacklog();
    if (server.gtid_seq != NULL) {
        gtidSeqDestroy(server.gtid_seq);
        server.gtid_seq = NULL;
    }
}

/* Reset will not create backlog if it not exists */
void ctrip_resetReplicationBacklog(void) {
    /* See resizeReplicationBacklog for more details */
    if (server.repl_backlog != NULL) {
        zfree(server.repl_backlog);
        server.repl_backlog = zmalloc(server.repl_backlog_size);
        server.repl_backlog_histlen = 0;
        server.repl_backlog_idx = 0;
        server.repl_backlog_off = server.master_repl_offset+1;
    }
    /* gtid_seq became invalid if master offset bumped. */
    if (server.gtid_seq != NULL) {
        gtidSeqDestroy(server.gtid_seq);
        server.gtid_seq = gtidSeqCreate();
    }
}

void ctrip_replicationFeedSlaves(list *slaves, int dictid, robj **argv,
        int argc, const char *uuid, size_t uuid_len, gno_t gno, long long offset) {
    int touch_index = uuid != NULL && gno >= GTID_GNO_INITIAL && server.gtid_seq
        && server.masterhost == NULL;
#ifdef ENABLE_SWAP
    touch_index = touch_index && server.swap_draining_master == NULL;
#endif
    if (touch_index) gtidSeqAppend(server.gtid_seq,uuid,uuid_len,gno,offset);
    replicationFeedSlaves(slaves,dictid,argv,argc);
    if (touch_index) gtidSeqTrim(server.gtid_seq,server.repl_backlog_off);
}

void ctrip_replicationFeedSlavesFromMasterStream(list *slaves, char *buf,
        size_t buflen, const char *uuid, size_t uuid_len, gno_t gno, long long offset) {
    int touch_index = uuid != NULL && gno >= GTID_GNO_INITIAL && server.gtid_seq;
    if (touch_index) gtidSeqAppend(server.gtid_seq,uuid,uuid_len,gno,offset);
    replicationFeedSlavesFromMasterStream(slaves,buf,buflen);
    if (touch_index) gtidSeqTrim(server.gtid_seq,server.repl_backlog_off);
}

void gtidInitialInfoInit(gtidInitialInfo *info) {
    info->gtid_lost = NULL;
    info->master_uuid = NULL;
    info->replid = NULL;
    info->reploff = 0;
}

void gtidInitialInfoSetup(gtidInitialInfo *info, gtidSet *gtid_lost,
        sds master_uuid, sds replid, long long reploff) {
    if (info->gtid_lost) gtidSetFree(info->gtid_lost);
    if (info->master_uuid) sdsfree(info->master_uuid);
    if (info->replid) sdsfree(info->replid);
    info->gtid_lost = gtid_lost;
    info->master_uuid = master_uuid;
    info->replid = replid;
    info->reploff = reploff;
}

/* On some edge cases, master might reply psync with unexpected reply,
 * and replica can't successfully sync with master, i.e. master link broken.
 * To help replica create replication link, we force replica to issue
 * force fullresync request. */
void serverReplStreamMasterLinkBroken() {
    if (server.repl_mode->mode == REPL_MODE_XSYNC) {
        forceXsyncFullResync();
    } else {
        replicationDiscardCachedMaster();
    }
}

void serverReplStreamResurrectCreate(connection *conn, int dbid,
        const char *replid, long long reploff) {
    serverAssert(server.cached_master == NULL);
    serverAssert(replid != NULL && reploff >= 0);

    memcpy(server.master_replid,replid,sizeof(server.master_replid));
    server.master_initial_offset = reploff;
    replicationCreateMasterClient(conn,dbid);
    server.repl_state = REPL_STATE_CONNECTED;
    server.repl_down_since = 0;

    /* Fire the master link modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
            REDISMODULE_SUBEVENT_MASTER_LINK_UP,NULL);

    if (server.repl_backlog == NULL) ctrip_createReplicationBacklog();
}

/* See disconnectSlaves for more details */
static void disconnectSlavesExcept(client *trigger_slave) {
    listIter li;
    listNode *ln;
    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = (client *)ln->value;
        if (slave == trigger_slave) continue;
        sds client_desc = catClientInfoString(sdsempty(), slave);
        serverLog(LL_NOTICE,
                "[gtid] Disconnect slave to notify gtid.set-lost update: %s",
                client_desc);
        writeToClient(slave,0);
        if (clientHasPendingReplies(slave)) {
            serverLog(LL_NOTICE,
                    "[gtid] Slave have pending replies when disconnect: %s",
                    client_desc);
        }
        freeClient(slave);
        sdsfree(client_desc);
    }
}

/* gtid.lost could be update by:
 * - GTIDX command
 * - XSYNC request by replica
 * - XCONTINUE reply by master
 * mater.uuid, replid, reploff could be update by:
 * - XCONTINUE reply */
void serverReplStreamUpdateXsync(gtidSet *delta_lost, client *trigger_slave,
        sds master_uuid, sds replid, long long reploff) {
    int from_master = trigger_slave == NULL, updated = 0,
        mode = server.repl_mode->mode;
    long long myreploff;

    if (gtidSetCount(delta_lost) != 0) {
        sds gtid_lost_repr = NULL, gtid_delta_lost_repr = NULL,
            gtid_updated_lost_repr = NULL;
        gtid_lost_repr = gtidSetDump(server.gtid_lost);
        gtid_delta_lost_repr = gtidSetDump(delta_lost);

        serverGtidSetAddLost(delta_lost);

        gtid_updated_lost_repr = gtidSetDump(server.gtid_lost);
        serverLog(LL_NOTICE, "[gtid] gtid.set-lost update: gtid.set-lost(%s)"
                " = gtid.set-lost(%s) + gtid.set-delta_lost(%s)",
                gtid_updated_lost_repr, gtid_lost_repr, gtid_delta_lost_repr);

        sdsfree(gtid_lost_repr), sdsfree(gtid_delta_lost_repr);
        sdsfree(gtid_updated_lost_repr);
        updated = 1;
    }

    if (master_uuid &&
            (sdslen(master_uuid) != server.master_uuid_len ||
             memcmp(master_uuid,server.master_uuid,server.master_uuid_len))) {
        serverAssert(from_master && mode == REPL_MODE_XSYNC);
        serverLog(LL_NOTICE, "[gtid] master.uuid updated: %s => %s",
                server.master_uuid,master_uuid);
        memcpy(server.master_uuid,master_uuid,sdslen(master_uuid));
        updated = 1;
    }

    if (replid && memcmp(server.replid,replid,sdslen(replid)) ) {
        serverAssert(from_master && mode == REPL_MODE_XSYNC);
        serverLog(LL_NOTICE, "[gtid] replid updated: %s => %s",
                server.replid,replid);
        memcpy(server.replid,replid,sdslen(replid));
        updated = 1;
    }

    myreploff = ctrip_getMasterReploff();
    if (reploff > 0 && reploff != myreploff) {
        serverAssert(from_master && mode == REPL_MODE_XSYNC);
        ctrip_setMasterReploff(reploff);
        serverLog(LL_NOTICE, "[gtid] master reploff updated: %lld => %lld",
                myreploff,reploff);
        updated = 1;
    }

    if (updated) {
        if (server.masterhost && !from_master) {
            serverLog(LL_NOTICE, "[gtid] reconnect with master to sync repl stream update");
            if (server.master) freeClientAsync(server.master);
            cancelReplicationHandshake(0);
        }

        if (listLength(server.slaves)) {
            serverLog(LL_NOTICE, "[gtid] disconnect slaves to sync repl stream update");
            disconnectSlavesExcept(trigger_slave);
        }
    }
}

/* switch repl mode if needed when gtid config takes effect:
 * - master change gtid-enabled config
 * - slave promoted as master */
void serverReplStreamSwitchIfNeeded(int to_mode, int flags,
        const char *log_prefix) {
    char replid[CONFIG_RUN_ID_SIZE+1] = {0};
    int from_mode = server.repl_mode->mode;
    if (from_mode == to_mode) return;
    getRandomHexChars(replid,CONFIG_RUN_ID_SIZE);
    if (to_mode == REPL_MODE_PSYNC) {
        serverReplStreamSwitch2Psync(replid,server.master_repl_offset,
                flags,log_prefix);
    } else {
        serverReplStreamSwitch2Xsync(replid,server.master_repl_offset,
                server.master_uuid,flags,log_prefix);
    }
}

static void serverReplStreamDisconnectSlaves(const char *log_prefix) {
    serverLog(LL_NOTICE, "[gtid] Disconnect slaves to notify %s.",
            log_prefix);
    disconnectSlaves();
    server.repl_no_slaves_since = server.unixtime;
}

/* server repl stream could switch to psync:
 * - slave(xsync) request got continue reply
 * - master gtid-enabled changed from yes to no
 * - slave(xsync) promoted as master with gtid-enabled */
void serverReplStreamSwitch2Psync(const char *replid, long long reploff,
        int flags, const char *log_prefix) {
    serverAssert(server.repl_mode->mode == REPL_MODE_XSYNC);
    serverAssert(!server.master && !server.cached_master);
    serverAssert(replid != NULL && reploff >= 0);

    if (server.master_repl_offset != reploff) {
        /* discard backlog when master_repl_offset bump. */
        serverLog(LL_NOTICE, "[gtid] switch repl stream to psync: "
                "backlog discared, master_repl_offset(%lld) != reploff(%lld)",
                server.master_repl_offset, reploff);
        server.master_repl_offset = reploff;
        ctrip_resetReplicationBacklog();
    } else {
        serverLog(LL_NOTICE, "[gtid] switch repl stream to psync:"
                "backlog reserved, master_repl_offset(%lld) == reploff(%lld)",
                server.master_repl_offset, reploff);
    }

    ctrip_setMasterReploff(reploff);
    serverAssert(server.gtid_reploff_delta == 0);

    shiftServerReplMode(REPL_MODE_PSYNC,log_prefix);

    memcpy(server.replid,replid,sizeof(server.replid));
    clearReplicationId2();
    server.slaveseldb = -1;

    clearMasterUuid();
    xsyncUuidInterestedInit();

    if (flags & RS_UPDATE_DOWN) serverReplStreamDisconnectSlaves(log_prefix);
}

/* server repl stream could switch to xsync:
 * - slave(psync) request got xcontinue reply
 * - master gtid-enabled changed from no to yes
 * - slave(psync) promoted as master with gtid disabled */
void serverReplStreamSwitch2Xsync(sds replid, long long reploff,
        sds master_uuid, int flags, const char *log_prefix) {
    serverAssert(server.repl_mode->mode == REPL_MODE_PSYNC);
    serverAssert(server.gtid_reploff_delta == 0);

    shiftServerReplMode(REPL_MODE_XSYNC,log_prefix);

    memcpy(server.replid,replid,sizeof(server.replid));
    clearReplicationId2();
    server.slaveseldb = -1;

    xsyncUuidInterestedInit();
    ctrip_setMasterReploff(reploff);
    setMasterUuid(master_uuid,sdslen(master_uuid));

    replicationDiscardCachedMaster();

    if (flags & RS_UPDATE_DOWN) serverReplStreamDisconnectSlaves(log_prefix);
}

/* serverReplStreamReset2Psync are scattered in readSyncBulkPayload,
 * loadDataFromDisk functions in order not to change redis code too much. */
void serverReplStreamReset2Psync(const char *replid, long long reploff,
        int flags, const char *log_prefix) {
    serverAssert(replid != NULL && reploff >= 0);

    server.master_repl_offset = reploff;
    ctrip_resetReplicationBacklog();

    resetServerReplMode(REPL_MODE_PSYNC,log_prefix);

    memcpy(server.replid,replid,sizeof(server.replid));
    clearReplicationId2();
    server.slaveseldb = -1;

    clearMasterUuid();
    xsyncUuidInterestedInit();
    server.gtid_reploff_delta = 0;

    if (flags & RS_UPDATE_DOWN) serverReplStreamDisconnectSlaves(log_prefix);
}

/* server repl stream could reset to xsync:
 * - load data from disk when server start.
 * - fullresync from master after readSyncBulkLoad. */
void serverReplStreamReset2Xsync(sds replid, long long reploff,
        int repl_stream_db, sds master_uuid, gtidSet *gtid_executed,
        gtidSet *gtid_lost, int flags, const char *log_prefix) {
    UNUSED(flags);
    /* Reset repl stream to xsync.
     * Note that replid, reploff may not be saved in RDB by redis instance
     * if it was not in any replication stream. Generate a new one in this
     * case. */
    if (reploff >= 0) server.master_repl_offset = reploff;
    ctrip_resetReplicationBacklog();

    resetServerReplMode(REPL_MODE_XSYNC,log_prefix);
    xsyncUuidInterestedInit();

    clearReplicationId2();
    if (replid == NULL)
        changeReplicationId();
    else
        memcpy(server.replid,replid,sizeof(server.replid));

    server.slaveseldb = repl_stream_db;

    if (master_uuid) {
        memcpy(server.master_uuid,master_uuid,sdslen(master_uuid));
        server.master_uuid_len = sdslen(master_uuid);
    }

    serverGtidSetResetExecuted(gtidSetDup(gtid_executed));
    serverGtidSetResetLost(gtidSetDup(gtid_lost));
}

