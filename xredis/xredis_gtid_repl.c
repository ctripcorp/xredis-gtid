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
#include "xredis_gtid_adaptation_version.h"


int replicationSetupSlaveForXFullResync(client *slave, long long offset) {
    int ret = C_OK;
    sds gtid_lost_repr = NULL, repr = NULL;
    size_t master_uuid_len = 0;
    const char *master_uuid = getMasterUuid(&master_uuid_len);

    repr = sdsnew("+XFULLRESYNC");

    gtid_lost_repr = gtidSetQuoteIfEmpty(gtidSetDump(server.gtid_lost));
    repr = sdscat(repr," GTID.LOST ");
    repr = sdscatlen(repr,gtid_lost_repr,sdslen(gtid_lost_repr));

    repr = sdscat(repr," MASTER.UUID ");
    repr = sdscatlen(repr,master_uuid,master_uuid_len);

    repr = sdscat(repr," REPLID ");
    repr = sdscat(repr,server.replid);

    repr = sdscat(repr," REPLOFF ");
    sds reploff = sdsfromlonglong(ctrip_getMasterReploff());
    repr = sdscatsds(repr,reploff);
    sdsfree(reploff);

    repr = sdscat(repr, "\r\n");

    slave->psync_initial_offset = offset;
    slave->replstate = SLAVE_STATE_WAIT_BGSAVE_END;
    /* We are going to accumulate the incremental changes for this
     * slave as well. Set slaveseldb to -1 in order to force to re-emit
     * a SELECT statement in the replication stream. */
    server.slaveseldb = -1;

    /* Don't send this reply to slaves that approached us with
     * the old SYNC command. */
    if (!(slave->flags & CLIENT_PRE_PSYNC)) {
        if (connWrite(slave->conn,repr,sdslen(repr)) != (int)sdslen(repr)) {
            freeClientAsync(slave);
            ret = C_ERR;
            goto end;
        }
    }

end:
    sdsfree(gtid_lost_repr), sdsfree(repr);
    return ret;
}

int ctrip_replicationSetupSlaveForFullResync(client *slave, long long offset) {
    if (server.repl_mode->mode != REPL_MODE_XSYNC)
        return replicationSetupSlaveForFullResync(slave, offset);
    else
        return replicationSetupSlaveForXFullResync(slave, offset);
}

#define GTID_XSYNC_MAX_REPLY_SIZE (64*1024)

/* XCONTINUE reply could exceed 256 byte. */
char *ctrip_receiveSynchronousResponse(connection *conn) {
    char *buf = zcalloc(GTID_XSYNC_MAX_REPLY_SIZE);
    if (connSyncReadLine(conn,buf,GTID_XSYNC_MAX_REPLY_SIZE,
                server.repl_syncio_timeout*1000) == -1)
    {
        zfree(buf);
        return sdscatprintf(sdsempty(),"-Reading from master: %s",
                strerror(errno));
    }
    server.repl_transfer_lastio = server.unixtime;
    sds response = sdsnew(buf);
    zfree(buf);
    return response;
}

typedef struct syncRequest {
    int mode;
    union {
        struct {
            sds replid;
            long long offset;
        } p; /* psync */
        struct {
            sds uuid_interested;
            gtidSet *gtid_slave;
            gtidSet *gtid_lost;
            long long maxgap;
        } x; /* xsync */
        struct {
            sds msg;
        } i; /* invalid */
    };
} syncRequest;

syncRequest *syncRequestNew() {
    syncRequest *request = zcalloc(sizeof(syncRequest));
    request->mode = REPL_MODE_UNSET;
    return request;
}

void syncRequestFree(syncRequest *request) {
    if (request == NULL) return;
    switch (request->mode) {
    case REPL_MODE_PSYNC:
        sdsfree(request->p.replid);
        break;
    case REPL_MODE_XSYNC:
        sdsfree(request->x.uuid_interested);
        gtidSetFree(request->x.gtid_slave);
        gtidSetFree(request->x.gtid_lost);
        break;
    case REPL_MODE_UNSET:
        sdsfree(request->i.msg);
        break;
    default:
        serverPanic("unexpected repl mode");
        break;
    }
    zfree(request);
}

void masterParsePsyncRequest(syncRequest *request, robj *replid, robj *offset) {
    long long value;
    if (getLongLongFromObject(offset,&value) != C_OK) {
        request->mode = REPL_MODE_UNSET;
        request->i.msg = sdscatprintf(sdsempty(),"offset %s invalid",(sds)offset->ptr);
    } else {
        request->mode = REPL_MODE_PSYNC;
        request->p.replid = sdsdup(replid->ptr);
        request->p.offset = value;
    }
}

void masterParseXsyncRequest(syncRequest *request, robj *uuid, robj *gtidset,
        int optargc, robj **optargv) {
    long long maxgap = 0;
    gtidSet *gtid_slave = NULL, *gtid_lost = NULL;
    sds gtid_repr = gtidset->ptr, msg = NULL;

    if ((gtid_slave = gtidSetDecode(gtid_repr,sdslen(gtid_repr))) == NULL) {
        msg = sdscatprintf(sdsempty(), "invalid gtid.set %s", gtid_repr);
        goto invalid;
    }

    for (int i = 0; i+1 < optargc; i += 2) {
        if (!strcasecmp(optargv[i]->ptr,"maxgap")) {
            if (getLongLongFromObject(optargv[i+1],&maxgap) != C_OK) {
                maxgap = 0;
                serverLog(LL_NOTICE, "Ignored invalid xsync maxgap option: %s",
                        (sds)optargv[i+1]->ptr);
            }
        } else if (!strcasecmp(optargv[i]->ptr,"gtid.lost")) {
            if ((gtid_lost = gtidSetDecode(optargv[i+1]->ptr,
                            sdslen(optargv[i+1]->ptr))) == NULL) {
                serverLog(LL_WARNING, "Invalid xsync gtid.lost option: %s",
                        (sds)optargv[i+1]->ptr);
                goto invalid;
            }
        } else {
            serverLog(LL_NOTICE, "Ignored invalid xsync option %s",
                    (sds)optargv[i]->ptr);
        }
    }

    request->mode = REPL_MODE_XSYNC;
    request->x.gtid_slave = gtid_slave;
    request->x.gtid_lost = gtid_lost;
    if (request->x.gtid_lost == NULL) {
        serverLog(LL_NOTICE, "gtid.lost unspecified, default to empty");
        request->x.gtid_lost = gtidSetNew();
    }
    request->x.maxgap = maxgap;
    request->x.uuid_interested = sdsnew(uuid->ptr);
    return;

invalid:
    if (gtid_slave) gtidSetFree(gtid_slave);
    if (gtid_lost) gtidSetFree(gtid_lost);
    request->mode = REPL_MODE_UNSET;
    request->i.msg = msg;
    return;
}

syncRequest *masterParseSyncRequest(client *c) {
    syncRequest *request = syncRequestNew();
    char *mode = c->argv[0]->ptr;
    sds cmdrepr = sdsempty();

    for (int i = 0; i < c->argc; i++) {
        if (c->argv[i]->encoding == OBJ_ENCODING_INT) {
            cmdrepr = sdscatprintf(cmdrepr, "\"%ld\"", (long)c->argv[i]->ptr);
        } else {
            cmdrepr = sdscatrepr(cmdrepr,(char*)c->argv[i]->ptr,
                        sdslen(c->argv[i]->ptr));
        }
        if (i != c->argc-1) cmdrepr = sdscatlen(cmdrepr," ",1);
    }
    serverLog(LL_NOTICE,
            "[gtid] replica %s asks for synchronization with request: %s",
            replicationGetSlaveName(c), cmdrepr);
    sdsfree(cmdrepr);

    if (!strcasecmp(mode,"psync")) {
        masterParsePsyncRequest(request,c->argv[1],c->argv[2]);
    } else if (!strcasecmp(mode,"xsync")) {
        masterParseXsyncRequest(request,c->argv[1],c->argv[2],c->argc-3,c->argv+3);
    } else {
        request->mode = REPL_MODE_UNSET;
        request->i.msg = sdscatprintf(sdsempty(), "invalid repl mode: %s", mode);
    }
    return request;
}

#define SYNC_ACTION_NOP         0
#define SYNC_ACTION_XCONTINUE   1
#define SYNC_ACTION_CONTINUE    2
#define SYNC_ACTION_FULL        3

typedef struct syncResult {
    int request_mode;
    int action;
    long long offset; /* send offset start */
    long long limit; /* send offset limit */
    sds msg;
    union {
        /* Note that there's no need to save replid/gtid.lost for fullresync
         * or xfullresync, fullresync should use current replid/gtid.lost
         * save in server instead of replid/gtid snapshot when ana reqeust. */
        struct {
            sds replid;
            long long reploff;
            gtidSet *gtid_cont;
            gtidSet *delta_lost;
        } xc; /* xcontinue */
        struct {
            sds replid;
            /* reploff that slave has logically received. will be propagate
             * to slave when xcontinue => continue to align repl reploff
             * of master and slave.
             * Note that to keep compatible with origin replication proptocol,
             * when reploff < 0, it will not be propragte to slave. */
            long long reploff;
            /* gtid.set-lost might exists when xsync => psync. */
            gtidSet *delta_lost;
        } cc; /* continue */
    };
} syncResult;

syncResult *syncResultNew() {
    syncResult *result = zcalloc(sizeof(syncResult));
    return result;
}

void syncResultFree(syncResult *result) {
    if (result == NULL) return;
    switch (result->action) {
    case SYNC_ACTION_NOP:
        break;
    case SYNC_ACTION_XCONTINUE:
        sdsfree(result->xc.replid);
        gtidSetFree(result->xc.gtid_cont);
        gtidSetFree(result->xc.delta_lost);
        break;
    case SYNC_ACTION_CONTINUE:
        sdsfree(result->cc.replid);
        gtidSetFree(result->cc.delta_lost);
        break;
    case SYNC_ACTION_FULL:
        break;
    }
    sdsfree(result->msg);
    zfree(result);
}

void masterAnaPsyncRequest(syncResult *result, syncRequest *request) {
    syncLocateResult slr;
    sds psync_replid = request->p.replid;
    long long psync_offset = request->p.offset;

    if (request->p.replid[0] == '?') {
        result->action = SYNC_ACTION_FULL;
        result->msg = sdsnew("fullresync request");
        return;
    }

    if (!server.repl_backlog ||
        psync_offset < gtidGetBacklogOffset()  ||
        psync_offset > (gtidGetBacklogOffset() +gtidGetBacklogHistlen())) {
        result->action = SYNC_ACTION_FULL;
        result->msg = sdscatprintf(sdsempty(),
                "psync offset(%lld) not in backlog [%lld,%lld)",
                psync_offset, gtidGetBacklogOffset() ,
                gtidGetBacklogOffset() +gtidGetBacklogHistlen());
        return;
    }

    result->offset = psync_offset;

    syncLocateResultInit(&slr);
    locateServerReplMode(REPL_MODE_PSYNC,psync_offset,&slr);

    if (slr.locate_type == LOCATE_TYPE_INVALID) {
        result->action = SYNC_ACTION_FULL;
        result->msg = slr.i.msg, slr.i.msg = NULL;
    } else if (slr.locate_type == LOCATE_TYPE_PREV ||
            slr.locate_type == LOCATE_TYPE_SWITCH) {
        serverAssert(server.prev_repl_mode->mode == REPL_MODE_PSYNC);
        const char *replid1 = server.prev_repl_mode->psync.replid;
        const char *replid2 = server.prev_repl_mode->psync.replid2;
        long long offset1 = server.repl_mode->from;
        long long offset2 = server.prev_repl_mode->psync.second_replid_offset;
        if ((!strcasecmp(psync_replid, replid1) && psync_offset <= offset1) ||
            (!strcasecmp(psync_replid, replid2) && psync_offset <= offset2)) {
            if (slr.locate_type == LOCATE_TYPE_PREV) {
                result->action = SYNC_ACTION_CONTINUE;
                result->limit = slr.p.limit;
                result->cc.replid = sdsnew(replid1);
                result->cc.reploff = -1; /* no need to align reploff */
                result->msg = sdsnew("prior psync => xsync");
            } else {
                gtidSet *gtid_master, *gtid_cont, *gtid_xsync;
                sds gtid_master_repr, gtid_continue_repr, gtid_xsync_repr;

                gtid_master = serverGtidSetGet("[psync] [ana]");
                gtid_master_repr = gtidSetDump(gtid_master);

                gtid_xsync = gtidSeqPsync(server.gtid_seq,psync_offset);
                gtid_cont = gtid_master, gtid_master = NULL;
                gtidSetDiff(gtid_cont,gtid_xsync);

                gtid_continue_repr = gtidSetDump(gtid_cont);
                gtid_xsync_repr = gtidSetDump(gtid_xsync);
                serverLog(LL_NOTICE, "[psync] gtid.set-continue(%s) ="
                        " gtid.set-master(%s) - gtid.set-xsync(%s)",
                        gtid_continue_repr,gtid_master_repr,gtid_xsync_repr);

                result->action = SYNC_ACTION_XCONTINUE;
                result->xc.replid = sdsnew(serverReplModeGetCurReplIdOff(
                            psync_offset-1,&result->xc.reploff));
                result->xc.gtid_cont = gtid_cont, gtid_cont = NULL;
                result->xc.delta_lost = gtidSetNew();
                result->msg = sdsnew("psync => xsync");

                sdsfree(gtid_master_repr);
                sdsfree(gtid_continue_repr);
                sdsfree(gtid_xsync_repr);

                gtidSetFree(gtid_xsync);
            }
        } else {
            result->action = SYNC_ACTION_FULL;
            result->msg = sdscatprintf(sdsempty(),
                    "(%s:%lld) can't continue in {(%s:%lld),(%s:%lld)}",
                    psync_replid, psync_offset,
                    replid1, offset1, replid2, offset2);
        }
    } else {
        /* Let origin redis hanle this psync request */
        serverAssert(slr.locate_type == LOCATE_TYPE_CUR);
        result->action = SYNC_ACTION_NOP;
    }

    syncLocateResultDeinit(&slr);
}

void masterAnaXsyncRequest(syncResult *result, syncRequest *request) {
    syncLocateResult slr;
    long long psync_offset, maxgap = request->x.maxgap;
    gtidSet *gtid_slave = request->x.gtid_slave;
    gtidSet *gtid_master = NULL, *gtid_cont = NULL, *gtid_xsync = NULL,
            *gtid_gap = NULL, *gtid_mlost = NULL, *gtid_slost = NULL,
            *gtid_mexec = NULL, *gtid_sexec = NULL,
            *gtid_mexec_gap = NULL, *gtid_sexec_gap = NULL;
    sds gtid_master_repr = NULL, gtid_continue_repr = NULL,
        gtid_xsync_repr = NULL, gtid_slave_repr = NULL,
        gtid_mlost_repr = NULL, gtid_slost_repr = NULL,
        gtid_mexec_repr = NULL, gtid_sexec_repr = NULL,
        gtid_mgap_repr = NULL, gtid_sgap_repr = NULL,
        gtid_lost_repr = NULL, gtid_executed_repr = NULL;

    syncLocateResultInit(&slr);

    if (!strcmp(request->x.uuid_interested,
                GTID_XSYNC_UUID_INTERESTED_FULLRESYNC)) {
        result->action = SYNC_ACTION_FULL;
        result->msg = sdsnew("xfullresync requested");
        goto end;
    }

    gtid_master = serverGtidSetGet("[xsync] [ana]");
    gtid_master_repr = gtidSetDump(gtid_master);
    gtid_slave_repr = gtidSetDump(gtid_slave);

    /* FullResync if gtidSet not related, for example:
     *   empty slave asks for xsync
     *   instance of another shard asks for xsync */
    if (!gtidSetRelated(gtid_master,gtid_slave)) {
        result->action = SYNC_ACTION_FULL;
        result->msg = sdscatprintf(sdsempty(),
                "gtid.set-master(%s) and gtid.set-slave(%s) not related",
                gtid_master_repr, gtid_slave_repr);
        goto end;
    }

    if (server.gtid_seq == NULL) {
        gtid_xsync = gtidSetNew();
        psync_offset = server.master_repl_offset+1;
        serverLog(LL_NOTICE, "[xsync] [ana] continue point defaults"
                " to backlog tail: gtid.seq not exists.");
    } else {
        psync_offset = gtidSeqXsync(server.gtid_seq,gtid_slave,&gtid_xsync);
    }

    gtid_xsync_repr = gtidSetDump(gtid_xsync);
    serverLog(LL_NOTICE, "[xsync] [ana] continue point locate at offset=%lld,"
            " gtid.set-xsync=%s", psync_offset, gtid_xsync_repr);

    if (psync_offset < 0) {
        if (server.repl_mode->mode == REPL_MODE_XSYNC) {
            psync_offset = server.master_repl_offset+1;
            serverLog(LL_NOTICE, "[xsync] [ana] continue point adjust to"
                    " backlog tail: offset=%lld", psync_offset);
        } else {
            psync_offset = server.repl_mode->from;
            serverLog(LL_NOTICE, "[xsync] [ana] continue point adjust to"
                    " psync from: offset=%lld", psync_offset);
        }
    }
    if (server.repl_mode->mode == REPL_MODE_PSYNC && psync_offset < server.repl_backlog_off) {
        result->action = SYNC_ACTION_FULL;
        result->msg = sdscatprintf(sdsempty(),
                "psync offset(%lld) not in backlog [%lld,%lld)",
                psync_offset, server.repl_backlog_off,
                server.repl_backlog_off+server.repl_backlog_histlen);
        goto end;
    } 
    result->offset = psync_offset;
    locateServerReplMode(REPL_MODE_XSYNC,psync_offset,&slr);

    if (slr.locate_type == LOCATE_TYPE_INVALID) {
        result->action = SYNC_ACTION_FULL;
        result->msg = slr.i.msg, slr.i.msg = NULL;
        goto end;
    }

    gtidSetDiff(gtid_master,gtid_xsync);
    gtid_cont = gtid_master, gtid_master = NULL;

    gtid_continue_repr = gtidSetDump(gtid_cont);
    serverLog(LL_NOTICE, "[xsync] [ana] gtid.set-continue(%s) ="
            " gtid.set-master(%s) - gtid.set-xsync(%s)",
            gtid_continue_repr,gtid_master_repr,gtid_xsync_repr);

    gtid_slost = gtidSetDup(gtid_cont);
    gtidSetDiff(gtid_slost,gtid_slave);

    gtid_slost_repr = gtidSetDump(gtid_slost);
    serverLog(LL_NOTICE, "[xsync] [ana] gtid.set-slost(%s) ="
            " gtid.set-continue(%s) - gtid.set-slave(%s)",
            gtid_slost_repr,gtid_continue_repr,gtid_slave_repr);

    gtid_mlost = gtidSetDup(gtid_slave);
    gtidSetDiff(gtid_mlost,gtid_cont);

    gtid_mlost_repr = gtidSetDump(gtid_mlost);
    serverLog(LL_NOTICE, "[xsync] [ana] gtid.set-mlost(%s) ="
            " gtid.set-slave(%s) - gtid.set-continue(%s)",
            gtid_mlost_repr,gtid_slave_repr,gtid_continue_repr);

    gtid_executed_repr = gtidSetDump(server.gtid_executed);

    gtid_mexec = gtidSetDup(server.gtid_executed);
    gtidSetDiff(gtid_mexec, gtid_xsync);

    gtid_mexec_repr = gtidSetDump(gtid_mexec);
    serverLog(LL_NOTICE, "[xsync] [ana] gtid.set-mexec(%s) ="
            " gtid.set-executed(%s) - gtid.set-xsync(%s)",
            gtid_mexec_repr,gtid_executed_repr,gtid_xsync_repr);

    gtid_lost_repr = gtidSetDump(request->x.gtid_lost);

    gtid_sexec = gtidSetDup(gtid_slave);
    gtidSetDiff(gtid_sexec, request->x.gtid_lost);
    gtid_sexec_repr = gtidSetDump(gtid_sexec);
    serverLog(LL_NOTICE, "[xsync] [ana] gtid.set-sexec(%s) ="
            " gtid.set-slave(%s) - gtid.set-lost(%s)",
            gtid_sexec_repr, gtid_slave_repr, gtid_lost_repr);

    gtid_mexec_gap = gtidSetDup(gtid_mexec);
    gtidSetDiff(gtid_mexec_gap,gtid_sexec);
    gtid_mgap_repr = gtidSetDump(gtid_mexec_gap);
    serverLog(LL_NOTICE, "[xsync] [ana] gtid.set-mgap(%s) ="
            " gtid.set-mexec(%s) - gtid.set-sexec(%s)",
            gtid_mgap_repr, gtid_mexec_repr, gtid_sexec_repr);

    gtid_sexec_gap = gtidSetDup(gtid_sexec);
    gtidSetDiff(gtid_sexec_gap,gtid_mexec);
    gtid_sgap_repr = gtidSetDump(gtid_sexec_gap);
    serverLog(LL_NOTICE, "[xsync] [ana] gtid.set-sgap(%s) ="
            " gtid.set-sexec(%s) - gtid.set-mexec(%s)",
            gtid_sgap_repr, gtid_sexec_repr, gtid_mexec_repr);

    gno_t gap = gtidSetCount(gtid_mexec_gap) + gtidSetCount(gtid_sexec_gap);
    if (gap > maxgap) {
        result->action = SYNC_ACTION_FULL;
        result->msg = sdscatprintf(sdsempty(), "gap=%lld > maxgap=%lld",
                gap, maxgap);
        goto end;
    }

    if (slr.locate_type == LOCATE_TYPE_PREV) {
        result->action = SYNC_ACTION_XCONTINUE;
        result->limit = slr.p.limit;
        result->xc.replid = sdsnew(serverReplModeGetPrevReplIdOff(
                    psync_offset-1,&result->xc.reploff));
        result->xc.gtid_cont = gtid_cont, gtid_cont = NULL;
        result->xc.delta_lost = gtid_mlost, gtid_mlost = NULL;
        result->msg = sdscatprintf(sdsempty(),
                "gap=%lld <= maxgap=%lld",gap,maxgap);
    } else if (slr.locate_type == LOCATE_TYPE_SWITCH) {
        result->action = SYNC_ACTION_CONTINUE;
        result->cc.replid = sdsnew(server.replid);
        result->cc.reploff = psync_offset-1;
        result->cc.delta_lost = gtid_mlost, gtid_mlost = NULL;
        result->msg = sdsnew("xsync => psync");
    } else {
        serverAssert(slr.locate_type == LOCATE_TYPE_CUR);
        result->action = SYNC_ACTION_XCONTINUE;
        result->xc.replid = sdsnew(serverReplModeGetCurReplIdOff(
                    psync_offset-1,&result->xc.reploff));
        result->xc.gtid_cont = gtid_cont, gtid_cont = NULL;
        result->xc.delta_lost = gtid_mlost, gtid_mlost = NULL;
        result->msg = sdscatprintf(sdsempty(),
                "gap=%lld <= maxgap=%lld",gap,maxgap);
    }

end:
    syncLocateResultDeinit(&slr);

    sdsfree(gtid_master_repr), sdsfree(gtid_continue_repr);
    sdsfree(gtid_xsync_repr), sdsfree(gtid_slave_repr);
    sdsfree(gtid_mlost_repr), sdsfree(gtid_slost_repr);
    sdsfree(gtid_mexec_repr), sdsfree(gtid_sexec_repr);
    sdsfree(gtid_mgap_repr), sdsfree(gtid_sgap_repr);
    sdsfree(gtid_lost_repr), sdsfree(gtid_executed_repr);

    gtidSetFree(gtid_master), gtidSetFree(gtid_cont), gtidSetFree(gtid_xsync);
    gtidSetFree(gtid_gap), gtidSetFree(gtid_mlost), gtidSetFree(gtid_slost);
    gtidSetFree(gtid_mexec), gtidSetFree(gtid_sexec);
    gtidSetFree(gtid_mexec_gap), gtidSetFree(gtid_sexec_gap);
}

syncResult *masterAnaSyncRequest(syncRequest *request) {
    syncResult *result = syncResultNew();
    switch (request->mode) {
    case REPL_MODE_PSYNC:
        result->request_mode = REPL_MODE_PSYNC;
        masterAnaPsyncRequest(result,request);
        break;
    case REPL_MODE_XSYNC:
        result->request_mode = REPL_MODE_XSYNC;
        masterAnaXsyncRequest(result,request);
        break;
    case REPL_MODE_UNSET:
        result->request_mode = REPL_MODE_UNSET;
        result->action = SYNC_ACTION_FULL;
        result->msg = request->i.msg, request->i.msg = NULL;
        break;
    }
    return result;
}

long long addReplyReplicationBacklogLimited(client *c, long long offset,
        long long limit) {
    return consumeReplicationBacklogLimited(offset,limit,
            consumeReplicationBacklogLimitedAddReplyCb,c);
}

typedef struct copyCbPrivData {
    char *buf;
    long long added;
} copyCbPrivData;

static void consumeReplicationBacklogLimitedCopyCb(char *p,
        long long thislen, void *_pd) {
    copyCbPrivData *pd = _pd;
    memcpy(pd->buf + pd->added, p, thislen);
    pd->added += thislen;
}

/* Check adReplyReplicationBacklog for more details */
long long copyReplicationBacklogLimited(char *buf, long long offset,
        long long limit) {
    copyCbPrivData pd = {buf, 0};
    return consumeReplicationBacklogLimited(offset,limit,
            consumeReplicationBacklogLimitedCopyCb,&pd);
}

/* see masterTryPartialResynchronization for more details. */
void masterSetupPartialSynchronization(client *c, long long offset,
        long long limit, char *buf, int buflen) {
    long long sent;

    if (server.repl_backlog == NULL) ctrip_createReplicationBacklog();

    c->flags |= CLIENT_SLAVE;
    c->replstate = SLAVE_STATE_ONLINE;
    c->repl_ack_time = server.unixtime;
    gtidClearReplStartCmdStreamOnAck(c);
    
    listAddNodeTail(server.slaves,c);

    if (connWrite(c->conn,buf,buflen) != buflen) {
        freeClientAsync(c);
        return;
    }
    serverAssert(offset >= server.repl_backlog_off);
    if (limit > 0) {
        sent = addReplyReplicationBacklogLimited(c,offset,limit);
    } else {
        sent = addReplyReplicationBacklog(c,offset);
    }

    serverLog(LL_NOTICE,
        "[gtid] Sent %lld bytes of backlog starting from offset %lld limit %lld.",
        sent, offset, limit);

    if (limit > 0) {
        serverLog(LL_NOTICE,
                "[gtid] Disconnect slave %s to notify repl mode switched.",
                replicationGetSlaveName(c));
        gtidFreeClientAsync(c);
        return;
    }

    /* Note that we don't need to set the selected DB at server.slaveseldb
     * to -1 to force the master to emit SELECT:
     * a) xcontinue: db selectd by gtid argv
     * b) continue : db already saved in cached_master */

    refreshGoodSlavesCount();

    moduleFireServerEvent(REDISMODULE_EVENT_REPLICA_CHANGE,
                          REDISMODULE_SUBEVENT_REPLICA_CHANGE_ONLINE,
                          NULL);
}

int masterReplySyncRequest(client *c,  long long psync_offset, syncResult *result) {
    int ret = result->action == SYNC_ACTION_FULL ? C_ERR : C_OK;

    if (result->action == SYNC_ACTION_NOP) {
        serverLog(LL_NOTICE,
                "[%s] Partial sync request from %s handle by vanilla redis.",
                replModeName(result->request_mode), replicationGetSlaveName(c));
        ret = gtidMasterTryPartialResynchronization(c, psync_offset);
    } else if (result->action == SYNC_ACTION_XCONTINUE) {
        char *buf;
        int buflen;
        const char *master_uuid;
        size_t master_uuid_len;
        sds gtid_cont_repr = gtidSetQuoteIfEmpty(gtidSetDump(result->xc.gtid_cont));
        sds gtid_lost_repr = gtidSetQuoteIfEmpty(gtidSetDump(server.gtid_lost));

        serverLog(LL_NOTICE,
                "[%s] Partial sync request from %s accepted: %s, "
                "offset=%lld, limit=%lld, gtid.set-cont=%s, gtid.set-lost=%s, master.uuid=%s",
                replModeName(result->request_mode),replicationGetSlaveName(c),
                result->msg, result->offset, result->limit,
                gtid_cont_repr, gtid_lost_repr, server.uuid);

        if (result->xc.delta_lost)
            serverReplStreamUpdateXsync(result->xc.delta_lost,c,NULL,NULL,-1);

        master_uuid = getMasterUuid(&master_uuid_len);

        buflen = sdslen(gtid_cont_repr) + sdslen(gtid_lost_repr) + 256;
        buf = zcalloc(buflen);
        buflen = snprintf(buf,buflen,
                "+XCONTINUE GTID.SET %.*s GTID.LOST %.*s MASTER.UUID %.*s "
                "REPLID %s REPLOFF %lld\r\n",
                (int)sdslen(gtid_cont_repr),gtid_cont_repr,
                (int)sdslen(gtid_lost_repr),gtid_lost_repr,
                (int)master_uuid_len,master_uuid,
                result->xc.replid,result->xc.reploff);
        masterSetupPartialSynchronization(c,result->offset,
                result->limit,buf,buflen);

        sdsfree(gtid_cont_repr);
        sdsfree(gtid_lost_repr);
        zfree(buf);
    } else if (result->action == SYNC_ACTION_CONTINUE) {
        char buf[128];
        int buflen;

        serverLog(LL_NOTICE, "[%s] Partial sync request from %s accepted: %s, "
                "offset=%lld, limit=%lld, cc.replid=%s, cc.reploff=%lld",
                replModeName(result->request_mode),replicationGetSlaveName(c),
                result->msg, result->offset, result->limit,
                result->cc.replid, result->cc.reploff);

        if (result->cc.delta_lost)
            serverReplStreamUpdateXsync(result->cc.delta_lost,c,NULL,NULL,-1);

        if (result->cc.reploff < 0) {
            buflen = snprintf(buf,sizeof(buf),"+CONTINUE %s\r\n",
                    result->cc.replid);
        } else {
            buflen = snprintf(buf,sizeof(buf),"+CONTINUE %s %lld\r\n",
                    result->cc.replid, result->cc.reploff);
        }
        masterSetupPartialSynchronization(c,result->offset,
                result->limit,buf,buflen);
    } else {
        serverLog(LL_NOTICE, "[%s] Partial sync request from %s rejected: %s",
                replModeName(result->request_mode),replicationGetSlaveName(c),
                result->msg);
    }

    return ret;
}

int ctrip_masterTryPartialResynchronization(client *c, long long psync_offset) {
    syncRequest *request = masterParseSyncRequest(c);
    syncResult *result = masterAnaSyncRequest(request);
    int ret = masterReplySyncRequest(c, psync_offset, result);
    syncRequestFree(request);
    syncResultFree(result);
    return ret;
}

const char *xsyncUuidInterestedGet(void);

sds sendXsyncCommand(connection *conn) {

    char maxgap[32];
    gtidSet *gtid_slave = NULL;
    sds gtid_slave_repr, gtid_lost_repr;

    gtid_slave = serverGtidSetGet("[xsync]");
    gtid_slave_repr = gtidSetDump(gtid_slave);
    gtid_lost_repr = gtidSetDump(server.gtid_lost);
    const char *uuid_interested = xsyncUuidInterestedGet();

    snprintf(maxgap,sizeof(maxgap),"%lld",server.gtid_xsync_max_gap);
    serverLog(LL_NOTICE, "[xsync] Trying partial xsync with "
            "uuid_interested=%s, gtid.set=%s, gtid.lost=%s, maxgap=%s",
            uuid_interested,gtid_slave_repr,gtid_lost_repr,maxgap);

    sds reply = sendCommand(conn,"XSYNC",uuid_interested,
            gtid_slave_repr,"GTID.LOST",gtid_lost_repr,"MAXGAP",maxgap,NULL);
    gtidSetFree(gtid_slave);
    sdsfree(gtid_slave_repr);
    sdsfree(gtid_lost_repr);
    return reply;
} 

int ctrip_slaveTryPartialResynchronizationWrite(connection *conn) {
    int result = PSYNC_WAIT_REPLY;

    serverLog(LL_NOTICE, "[gtid] Trying parital sync in (%s) mode.",
            replModeName(server.repl_mode->mode));

    if (server.repl_mode->mode != REPL_MODE_XSYNC) return -1;

    sds reply = sendXsyncCommand(conn);
    if (reply != NULL) {
        serverLog(LL_WARNING,"[xsync] Unable to send XSYNC: %s", reply);
        sdsfree(reply);
        connSetReadHandler(conn, NULL);
        result = PSYNC_WRITE_ERROR;
    }

    return result;
}

#define SYNC_REPLY_INVALID          0
#define SYNC_REPLY_FULLRESYNC       1
#define SYNC_REPLY_CONTINUE         2
#define SYNC_REPLY_XFULLRESYNC      3
#define SYNC_REPLY_XCONTINUE        4
#define SYNC_REPLY_RDBCHANNELSYNC   5
#define SYNC_REPLY_TRANSERR         6
#define SYNC_REPLY_TRANSERR2        7

typedef struct parsedSyncReply {
    int type;
    union {
        struct {
            sds replid;
            long long reploff;
        } fullresync;
        struct {
            sds replid;
            int reploff_is_set;
            long long reploff;
        } pcontinue;
        struct {
            gtidSet *gtid_lost;
            sds master_uuid;
            sds replid;
            long long reploff;
        } xfullresync;
        struct {
            gtidSet *gtid_cont;
            gtidSet *gtid_lost; /* default to empty if no specified */
            sds master_uuid;
            sds replid;
            long long reploff;
        } xcontinue;
        struct {
            long long client_id;
        } rdbchannelsync;
        struct {
            sds errmsg;
        } invalid;
    };
} parsedSyncReply;

parsedSyncReply *parsedSyncReplyNew() {
    parsedSyncReply *parsed = zcalloc(sizeof(parsedSyncReply));
    return parsed;
}

void parsedSyncReplyFree(parsedSyncReply *parsed) {
    if (parsed == NULL) return;
    switch (parsed->type) {
    case SYNC_REPLY_FULLRESYNC:
        sdsfree(parsed->fullresync.replid);
        parsed->fullresync.replid = NULL;
        break;
    case SYNC_REPLY_CONTINUE:
        sdsfree(parsed->pcontinue.replid);
        parsed->pcontinue.replid = NULL;
        break;
    case SYNC_REPLY_XFULLRESYNC:
        gtidSetFree(parsed->xfullresync.gtid_lost);
        parsed->xfullresync.gtid_lost = NULL;
        sdsfree(parsed->xfullresync.master_uuid);
        parsed->xfullresync.master_uuid = NULL;
        sdsfree(parsed->xfullresync.replid);
        parsed->xfullresync.replid = NULL;
        break;
    case SYNC_REPLY_XCONTINUE:
        gtidSetFree(parsed->xcontinue.gtid_cont);
        parsed->xcontinue.gtid_cont = NULL;
        gtidSetFree(parsed->xcontinue.gtid_lost);
        parsed->xcontinue.gtid_lost = NULL;
        sdsfree(parsed->xcontinue.master_uuid);
        parsed->xcontinue.master_uuid = NULL;
        sdsfree(parsed->xcontinue.replid);
        parsed->xcontinue.replid = NULL;
        break;
    case SYNC_REPLY_INVALID:
        sdsfree(parsed->invalid.errmsg);
        parsed->invalid.errmsg = NULL;
        break;
    default:
        break;
    }
    zfree(parsed);
}

/* +FULLRESYNC <replid> <reploff> */
static void parseSyncReplyFullresync(sds reply, parsedSyncReply *parsed) {
    char *replid = NULL, *offset = NULL;
    sds errmsg = NULL;
    long long parsed_offset = -1;

    /* FULL RESYNC, parse the reply in order to extract the replid
     * and the replication offset. */
    replid = strchr(reply,' ');
    if (replid) {
        replid++;
        offset = strchr(replid,' ');
        if (offset) offset++;
    }
    if (!replid || !offset || (offset-replid-1) != CONFIG_RUN_ID_SIZE) {
        errmsg = sdscatprintf(sdsempty(),"replid invalid(%s)",reply);
        goto invalid;
    } else {
        parsed_offset = strtoll(offset,NULL,10);
        if (parsed_offset < 0) {
            errmsg = sdscatprintf(sdsempty(),"offset invalid(%s)",reply);
            goto invalid;
        }
    }

    parsed->type = SYNC_REPLY_FULLRESYNC;
    parsed->fullresync.replid =sdsnewlen(replid,offset-replid-1);
    parsed->fullresync.reploff = parsed_offset;
    return;

invalid:
    parsed->type = SYNC_REPLY_INVALID;
    parsed->invalid.errmsg = errmsg;
}

/* +CONTINUE [<replid>] [<reloff>] */
static void parseSyncReplyContinue(sds reply, parsedSyncReply *parsed) {
    sds errmsg = NULL;
    char *start, *end;
    long long reploff = 0;
    int reploff_is_set = 0;
    sds replid = NULL;

    start = reply+9;
    while(start[0] == ' ' || start[0] == '\t') start++;
    end = start;
    while(end[0] != ' ' && end[0] != '\t' &&
            end[0] != '\r' && end[0] != '\n' && end[0] != '\0') end++;

    if (end == start) goto end; /* +continue */
    if (end-start != CONFIG_RUN_ID_SIZE) goto invalid;
    replid = sdsnewlen(start,CONFIG_RUN_ID_SIZE);

    start = end;
    while(start[0] == ' ' || start[0] == '\t') start++;
    end = start;
    while(end[0] != '\r' && end[0] != '\n' && end[0] != '\0') end++;

    if (end == start) goto end; /* +continue replid */
    if (string2ll(start,end-start,&reploff) == 0) {
        errmsg = sdscatprintf(sdsempty(),"reploff invalid(%s)",reply);
        sdsfree(replid);
        goto invalid;
    }
    reploff_is_set = 1; /* +continue replid offset */

end:
    parsed->type = SYNC_REPLY_CONTINUE;
    parsed->pcontinue.replid = replid;
    parsed->pcontinue.reploff_is_set = reploff_is_set;
    parsed->pcontinue.reploff = reploff;
    return;

invalid:
    parsed->type = SYNC_REPLY_INVALID;
    parsed->invalid.errmsg = errmsg;
}

/* +XFULLRESYNC GTID.LOST <gtid.lost> MASTER.UUID <master-uuid>
 * REPLID <replid> REPLOFF <reploff> */
static void parseSyncReplyXfullresync(sds reply, parsedSyncReply *parsed) {
    sds *tokens, errmsg = NULL, replid = NULL, master_uuid = NULL;
    size_t token_off = 12;
    int i, ntoken;
    gtidSet *gtid_lost = NULL;
    long long reploff = -1;

    while (token_off < sdslen(reply) && isspace(reply[token_off]))
        token_off++;
    tokens = sdssplitargs(reply+token_off,&ntoken);

    for (i = 0; i+1 < ntoken; i += 2) {
        if (!strncasecmp(tokens[i], "gtid.lost", sdslen(tokens[i]))) {
            gtid_lost = gtidSetDecode(tokens[i+1],sdslen(tokens[i+1]));
            if (gtid_lost == NULL) {
                errmsg = sdscatprintf(sdsempty(),"invalid gtid.set-lost(%s)",
                        tokens[i+1]);
                goto invalid;
            }
        } else if (!strncasecmp(tokens[i], "master.uuid", sdslen(tokens[i]))) {
            master_uuid = sdsdup(tokens[i+1]);
        } else if (!strncasecmp(tokens[i], "replid", sdslen(tokens[i]))) {
            if (sdslen(tokens[i+1]) != CONFIG_RUN_ID_SIZE) {
                errmsg = sdscatprintf(sdsempty(),"invalid replid(%s)",
                        tokens[i+1]);
                goto invalid;
            }
            replid = sdsdup(tokens[i+1]);
        } else if (!strncasecmp(tokens[i], "reploff", sdslen(tokens[i]))) {
            reploff = strtoll(tokens[i+1],NULL,10);
            if (reploff < 0) {
                errmsg = sdscatprintf(sdsempty(),"invalid reploff(%s)",
                        tokens[i+1]);
                goto invalid;
            }
        } else {
            serverLog(LL_NOTICE,
                    "Ignore unrecognized xfullresync option: %s", tokens[i]);
        }
    }

    if (!master_uuid) {
        errmsg = sdsnew("master.uuid unspecified");
        goto invalid;
    }

    if (!gtid_lost) {
        errmsg = sdsnew("gtid.lost unspecified");
        goto invalid;
    }

    if (!replid) {
        errmsg = sdsnew("replid invalid or unspecified");
        goto invalid;
    }

    if (reploff < 0) {
        errmsg = sdsnew("reploff invalid or unspecified");
        goto invalid;
    }

    parsed->type = SYNC_REPLY_XFULLRESYNC;
    parsed->xfullresync.master_uuid = master_uuid;
    parsed->xfullresync.gtid_lost = gtid_lost;
    parsed->xfullresync.replid = replid;
    parsed->xfullresync.reploff = reploff;

    sdsfreesplitres(tokens,ntoken);
    return;

invalid:
    parsed->type = SYNC_REPLY_INVALID;
    parsed->invalid.errmsg = errmsg;

    sdsfreesplitres(tokens,ntoken);
    if (replid) sdsfree(replid);
    if (master_uuid) sdsfree(master_uuid);
    if (gtid_lost) gtidSetFree(gtid_lost);
}

/* +XCONTINUE GTID.SET <gtid.set-continue> [GTID.LOST <gtid.set-lost>]
 * MASTER.UUID <master-uuid> REPLID <replid> REPLOFF <reploff> */
static void parseSyncReplyXcontinue(sds reply, parsedSyncReply *parsed) {
    sds *tokens, errmsg = NULL, replid = NULL, master_uuid = NULL;
    size_t token_off = 10;
    int i, ntoken;
    gtidSet *gtid_cont = NULL, *gtid_lost = NULL;
    long long reploff = -1;

    while (token_off < sdslen(reply) && isspace(reply[token_off]))
        token_off++;
    tokens = sdssplitlen(reply+token_off,
            sdslen(reply)-token_off, " ",1,&ntoken);

    for (i = 0; i+1 < ntoken; i += 2) {
        if (!strncasecmp(tokens[i], "gtid.set", sdslen(tokens[i]))) {
            gtid_cont = gtidSetDecode(tokens[i+1],sdslen(tokens[i+1]));
            if (gtid_cont == NULL) {
                errmsg = sdscatprintf(sdsempty(),"invalid gtid.set-cont(%s)",
                        tokens[i+1]);
                goto invalid;
            }
        } else if (!strncasecmp(tokens[i], "gtid.lost", sdslen(tokens[i]))) {
            gtid_lost = gtidSetDecode(tokens[i+1],sdslen(tokens[i+1]));
            if (gtid_lost == NULL) {
                errmsg = sdscatprintf(sdsempty(),"invalid gtid.set-lost(%s)",
                        tokens[i+1]);
                goto invalid;
            }
        } else if (!strncasecmp(tokens[i], "master.uuid", sdslen(tokens[i]))) {
            master_uuid = sdsdup(tokens[i+1]);
        } else if (!strncasecmp(tokens[i], "replid", sdslen(tokens[i]))) {
            if (sdslen(tokens[i+1]) != CONFIG_RUN_ID_SIZE) {
                errmsg = sdscatprintf(sdsempty(),"invalid replid(%s)",
                        tokens[i+1]);
                goto invalid;
            }
            replid = sdsdup(tokens[i+1]);
        } else if (!strncasecmp(tokens[i], "reploff", sdslen(tokens[i]))) {
            reploff = strtoll(tokens[i+1],NULL,10);
            if (reploff < 0) {
                errmsg = sdscatprintf(sdsempty(),"invalid reploff(%s)",
                        tokens[i+1]);
                goto invalid;
            }
        } else {
            serverLog(LL_NOTICE,
                    "Ignore unrecognized xcontinue option: %s", tokens[i]);
        }
    }

    if (!master_uuid) {
        errmsg = sdsnew("master.uuid unspecified");
        goto invalid;
    }

    if (!gtid_cont) {
        errmsg = sdsnew("gtid.set unspecified");
        goto invalid;
    }

    if (!replid) {
        errmsg = sdsnew("replid invalid or unspecified");
        goto invalid;
    }

    if (reploff < 0) {
        errmsg = sdsnew("reploff invalid or unspecified");
        goto invalid;
    }

    parsed->type = SYNC_REPLY_XCONTINUE;
    parsed->xcontinue.master_uuid = master_uuid;
    parsed->xcontinue.gtid_cont = gtid_cont;
    if (gtid_lost == NULL) {
        parsed->xcontinue.gtid_lost = gtidSetNew();
    } else {
        parsed->xcontinue.gtid_lost = gtid_lost;
        gtid_lost = NULL;
    }
    parsed->xcontinue.replid = replid;
    parsed->xcontinue.reploff = reploff;

    sdsfreesplitres(tokens,ntoken);
    return;

invalid:
    parsed->type = SYNC_REPLY_INVALID;
    parsed->invalid.errmsg = errmsg;

    sdsfreesplitres(tokens,ntoken);
    if (replid) sdsfree(replid);
    if (master_uuid) sdsfree(master_uuid);
    if (gtid_cont) gtidSetFree(gtid_cont);
    if (gtid_lost) gtidSetFree(gtid_lost);
}

/* Move parsed xfullresync reply to server.gtid_initial */
static void parsedSyncReplySetupGtidInital(parsedSyncReply *parsed) {
    serverAssert(parsed->type == SYNC_REPLY_XFULLRESYNC);
    gtidInitialInfoSetup(server.gtid_initial,
            parsed->xfullresync.gtid_lost,parsed->xfullresync.master_uuid,
            parsed->xfullresync.replid,parsed->xfullresync.reploff);
    parsed->xfullresync.gtid_lost = NULL;
    parsed->xfullresync.master_uuid = NULL;
    parsed->xfullresync.replid = NULL;
}

/* +RDBCHANNELSYNC clientid*/
static void parseSyncReplyRdbchannelsync(sds reply, parsedSyncReply *parsed) {
    char *client_id_str = strchr(reply,' ');
    sds errmsg = NULL;
    if (client_id_str)
        client_id_str++;

    if (!client_id_str) {
        errmsg = sdsnew("Master replied with wrong +RDBCHANNELSYNC syntax:");
        goto invalid;
    }
    long long client_id = strtoll(client_id_str, NULL, 10);
    parsed->type = SYNC_REPLY_RDBCHANNELSYNC;
    parsed->rdbchannelsync.client_id = client_id;
    return;

invalid:
    parsed->type = SYNC_REPLY_INVALID;
    parsed->invalid.errmsg = errmsg;
}

static parsedSyncReply *parseSyncReply(sds reply) {
    parsedSyncReply *parsed = parsedSyncReplyNew();

    if (!strncmp(reply,"+XFULLRESYNC",12)) {
        parseSyncReplyXfullresync(reply,parsed);
    } else if (!strncmp(reply,"+XCONTINUE",10)) {
        parseSyncReplyXcontinue(reply,parsed);
    } else if (!strncmp(reply,"+FULLRESYNC",11)) {
        parseSyncReplyFullresync(reply,parsed);
    } else if (!strncmp(reply,"+CONTINUE",9)) {
        parseSyncReplyContinue(reply,parsed);
    } else if(!strncmp(reply, "+RDBCHANNELSYNC",15)) {
        parseSyncReplyRdbchannelsync(reply,parsed);
    } else if (!strncmp(reply,"-NOMASTERLINK",13) ||
        !strncmp(reply,"-LOADING",8)) {
        parsed->type = SYNC_REPLY_TRANSERR;
    } else if (!strncmp(reply,"-Reading from master:",21)) {
        parsed->type = SYNC_REPLY_TRANSERR2;
    } else {
        parsed->type = SYNC_REPLY_INVALID;
        parsed->invalid.errmsg = sdsnew("invalid sync reply type");
    }

    return parsed;
}


/* read backlog iterator*/
#define ONCE_READ_BUF_SIZE 256
typedef struct readBacklogIterator {
    client mock;
    long long backlog;   /* -1 = not seeked yet; >=0 = backlog offset for mock.querybuf[mock.qb_pos] */
} readBacklogIterator;

void readBacklogIteratorInit(readBacklogIterator *it) {
    memset(&it->mock, 0, sizeof(it->mock));
    gtidMockClientInit(&it->mock);
    it->mock.bulklen = -1;  /* processMultibulkBuffer ） */
    it->backlog = -1;
}

void readBacklogIteratorDeinit(readBacklogIterator *it) {
    gtidMockClientDeinit(&it->mock);
    it->mock.querybuf = NULL;
    it->backlog = -1;  
}

void readBacklogIteratorSeekTo(readBacklogIterator *it, long long offset) {
    serverAssert(offset >= 0);  

    if (it->backlog < 0) {
        it->backlog = offset;
        return;
    }

    long long start = it->backlog - sdslen(it->mock.querybuf);
    long long end = it->backlog;

    if (offset == start) {
        return;  /* no-op */
    }
    if (offset >= start && offset < end) {
        size_t new_qb_pos =(size_t)(offset - (long long)start);
        sdsrange(it->mock.querybuf, new_qb_pos, -1);
        it->mock.qb_pos = 0;
        return;
    }
    /* offset < cur (rewind) or offset > end：clear+seek */
    sdsclear(it->mock.querybuf);
    it->mock.qb_pos = 0;
    it->backlog = offset;
}

ssize_t readBacklogIteratorParseNext(readBacklogIterator *it,
                                      robj ***out_argv, int *out_argc) {
    serverAssert(it->backlog >= 0);
    serverAssert(out_argv != NULL && out_argc != NULL);
    *out_argv = NULL;
    *out_argc = 0;

    gtidMockClientCleanArgv(&it->mock);

    size_t buffered = sdslen(it->mock.querybuf) - it->mock.qb_pos;
    size_t total_read = 0;
    int any_read = 0;

    while (1) {
        while (it->mock.qb_pos < sdslen(it->mock.querybuf)) {
            if (processMultibulkBuffer(&it->mock) != C_OK) {
                if (it->mock.flags & CLIENT_PROTOCOL_ERROR) {
                    serverLog(LL_WARNING,
                              "[gaplog] protocol error at offset %lld, qb_pos=%zu, flags=%lu",
                              it->backlog, it->mock.qb_pos,
                              (unsigned long)it->mock.flags);
                    return -1;
                }
                break;  
            }
            size_t consumed = buffered + total_read
                              - (sdslen(it->mock.querybuf) - it->mock.qb_pos);
            *out_argv = it->mock.argv;
            *out_argc = it->mock.argc;
            return (ssize_t)consumed;
        }

        ssize_t nread = gtidBacklogAppendToSds(it->backlog,
                                            &it->mock.querybuf,
                                            ONCE_READ_BUF_SIZE);
        if (nread <= 0) {
            if (!any_read) return 0; 
            serverLog(LL_WARNING,
                      "[gaplog] gtidBacklogAppendToSds failed mid-cmd at offset %lld",
                      it->backlog);
            
            return -1;
        }
        any_read = 1;
        total_read += nread;
        it->backlog += nread;
    }
}
typedef struct {
    robj **argv;
    int argc;
} gtidParsedCmd;
typedef struct {
    gtidParsedCmd *cmds;
    int num_cmds;
    int capacity;
} gtidParsedCmdList;

static void gtidParsedCmdListAdd(gtidParsedCmdList *list, client *c) {
    if (list->num_cmds >= list->capacity) {
        list->capacity = list->capacity ? list->capacity * 2 : 8;
        list->cmds = zrealloc(list->cmds,
                              sizeof(gtidParsedCmd) * list->capacity);
    }
    gtidParsedCmd *cmd = &list->cmds[list->num_cmds++];
    cmd->argv = c->argv;
    cmd->argc = c->argc;

    /* move */
    gtidMockClientMoveClientArgv(c);
}

static void gtidParsedCmdListCleanup(gtidParsedCmdList *list) {
    for (int i = 0; i < list->num_cmds; i++) {
        if (list->cmds[i].argv) {
            for (int j = 0; j < list->cmds[i].argc; j++)
                if (list->cmds[i].argv[j]) decrRefCount(list->cmds[i].argv[j]);
            zfree(list->cmds[i].argv);
        }
    }
    zfree(list->cmds);
}

void parseMultiCommand(gtidGaplogKeysBuilder *build,
                       readBacklogIterator *it,
                       long long select_dbid) {
    serverAssert(it->backlog >= 0);

    gtidParsedCmdList cmdlist = {0};

    while (1) {
        robj **argv;
        int argc;
        ssize_t consumed = readBacklogIteratorParseNext(it, &argv, &argc);
        if (consumed <= 0) break;
        serverAssert(argv != NULL && argc > 0);

        sds cmd0 = (sds)argv[0]->ptr;
        robj *argv3 = (argc >= 4) ? argv[3] : NULL;

        gtidParsedCmdListAdd(&cmdlist, &it->mock);

        if (argc >= 4 &&
            !strcasecmp(cmd0, "gtid") &&
            argv3 != NULL &&
            !strcasecmp((sds)argv3->ptr, "exec")) {
            break;
        }
    }

    serverAssert(cmdlist.num_cmds != 0);

    long long dbid = 0;
    if (select_dbid >= 0) {
        dbid = select_dbid;
    } else {
        gtidParsedCmd *last_cmd = &cmdlist.cmds[cmdlist.num_cmds - 1];
        if (last_cmd->argv && last_cmd->argv[0]) {
            sds last_cmd_name = (sds)last_cmd->argv[0]->ptr;
            if (!strcasecmp(last_cmd_name, "gtid") &&
                last_cmd->argc >= 3 && last_cmd->argv[2]) {
                getLongLongFromObject(last_cmd->argv[2], &dbid);
            }
        }
    }

    for (int i = 0; i < cmdlist.num_cmds - 1; i++) {
        gtidParsedCmd *cmd = &cmdlist.cmds[i];
        sds cmd_name = (sds)cmd->argv[0]->ptr;
        if (!strcasecmp(cmd_name, "select") && cmd->argc >= 2 && cmd->argv[1]) {
            getLongLongFromObject(cmd->argv[1], &dbid);
            continue;
        }
        gtidGaplogKeysBuilderAddFromCmd(build, dbid, cmd->argv, cmd->argc);
    }

    gtidParsedCmdListCleanup(&cmdlist);
}

int parseGtidCommand(gtidGaplogKeysBuilder *builder, robj **argv, int argc) {
    long long dbid = 0;

    if (argc < 4 || argv == NULL || argv[2] == NULL) {
        serverLog(LL_WARNING, "[gaplog] invalid GTID command, argc=%d", argc);
        return 0;
    }

    getLongLongFromObject(argv[2], &dbid);
    gtidGaplogKeysBuilderAddFromCmd(builder, dbid, argv + 3, argc - 3);
    return 0;  
}



void saveGapLogFromGtidSet(gtidSet *mlost) {
    readBacklogIterator it;
    readBacklogIteratorInit(&it);

    gtidSetIterator gs_iterator;
    gtidSetInitIterator(&gs_iterator, mlost);
    uuidSet *us = NULL;
    while ((us = gtidSetIteratorNext(&gs_iterator)) != NULL) {
        uuidSetIterator us_iterator;
        uuidSetInitIterator(&us_iterator, us);

        gtidIntervalNode *node = NULL;
        while ((node = uuidSetIteratorNext(&us_iterator)) != NULL) {
            sds uuid = sdsnewlen(us->uuid, us->uuid_len);
            for (gno_t gno = node->start; gno <= node->end; gno++) {
                long long offset = gtidSeqLookup(server.gtid_seq, uuid,
                                                  sdslen(uuid), gno);
                if (offset < 0) continue;

                readBacklogIteratorSeekTo(&it, offset);

                long long dbid_from_select = -1;
                gtidGaplogKeysBuilder builder = GTID_GAPLOG_KEYS_BUILDER_INIT;

                while (1) {
                    robj **argv;
                    int argc;
                    ssize_t consumed = readBacklogIteratorParseNext(&it, &argv, &argc);
                    if (consumed <= 0) break;

                    sds cmd_name = (sds)argv[0]->ptr;

                    if (!strcasecmp(cmd_name, "select") && argc >= 2) {
                        getLongLongFromObject(argv[1], &dbid_from_select);
                        continue;
                    }
                    if (!strcasecmp(cmd_name, "multi")) {
                        parseMultiCommand(&builder, &it, dbid_from_select);
                        break;
                    }
                    if (!strcasecmp(cmd_name, "gtid")) {
                        parseGtidCommand(&builder, argv, argc);
                        break;
                    }
                    serverLog(LL_WARNING, "[gaplog] saveGapLogFromGtidSet unexpected command %s", cmd_name);
                }

                if (builder.numkeys > 0) {
                    gtidGaplogInsert(server.gtid_gap_log, uuid, gno, gtidGaplogKeysBuild(&builder));
                }
                gtidGaplogDeinitKeysBuilder(&builder);
                
            }
            sdsfree(uuid);
        }
        uuidSetDeinitIterator(&us_iterator);
    }
    gtidSetDeinitIterator(&gs_iterator);

    readBacklogIteratorDeinit(&it);
}

int ctrip_slaveTryPartialResynchronizationRead(connection *conn, sds reply) {
    int result = PSYNC_BY_REDIS;

    serverLog(LL_NOTICE, "[gtid] got sync reply: %s",reply);

    parsedSyncReply *parsed = parseSyncReply(reply);

    if (parsed->type == SYNC_REPLY_TRANSERR) goto by_redis;
    if (parsed->type == SYNC_REPLY_TRANSERR2) {
        serverLog(LL_NOTICE,
                "[%s] Treat %s as transient error too, try psync again.",
                replModeName(server.repl_mode->mode), reply);
        result = PSYNC_TRY_LATER;
        goto end;
    }

    if (parsed->type == SYNC_REPLY_INVALID) {
        serverLog(LL_WARNING, "[%s] Parsed invalid reply(%s): "
                "fallback to fullresync",replModeName(server.repl_mode->mode),
                parsed->invalid.errmsg);
        serverReplStreamMasterLinkBroken();
        result = PSYNC_TRY_LATER;
        goto end;
    }

    if (parsed->type == SYNC_REPLY_RDBCHANNELSYNC) {
        goto by_redis;
    }

    if (server.repl_mode->mode != REPL_MODE_XSYNC) {
        if (parsed->type == SYNC_REPLY_XFULLRESYNC) {
            /* PSYNC => XFULLRESYNC */
            server.gtid_sync_stat[GTID_SYNC_PSYNC_XFULLRESYNC]++;
            serverLog(LL_NOTICE,
                    "[psync] repl mode switch: psync => xsync (xfullresync)");
            /* Repl mode will be reset on rdb loading */
            parsedSyncReplySetupGtidInital(parsed);
            replicationDiscardCachedMaster();
            result = PSYNC_FULLRESYNC;
        } else if (parsed->type == SYNC_REPLY_XCONTINUE) {
            /* PSYNC => XCONTINUE */
            server.gtid_sync_stat[GTID_SYNC_PSYNC_XCONTINUE]++;
            serverLog(LL_NOTICE,
                    "[psync] repl mode switch: psync => xsync (xcontinue)");

            /* align gtid.set with master */
            gtidSet *reply_executed = gtidSetDup(parsed->xcontinue.gtid_cont);
            gtidSetDiff(reply_executed,parsed->xcontinue.gtid_lost);

            sds gtid_executed_repr = gtidSetDump(server.gtid_executed);
            sds gtid_lost_repr = gtidSetDump(server.gtid_lost);
            sds reply_cont_repr = gtidSetDump(parsed->xcontinue.gtid_cont);
            sds reply_executed_repr = gtidSetDump(reply_executed);
            sds reply_lost_repr = gtidSetDump(parsed->xcontinue.gtid_lost);

            serverLog(LL_NOTICE,
                    "[psync] reply gtid.executed(%s) = gtid.cont(%s) - gtid.lost(%s)",
                    reply_executed_repr,reply_cont_repr,reply_lost_repr);

            serverLog(LL_NOTICE,"[psync] align my gtid.sets with master, "
                    "gtid.executed: %s => %s, gtid.lost: %s => %s",
                    gtid_executed_repr,reply_executed_repr,gtid_lost_repr,reply_lost_repr);

            sdsfree(reply_lost_repr);
            sdsfree(reply_executed_repr);
            sdsfree(reply_cont_repr);
            sdsfree(gtid_lost_repr);
            sdsfree(gtid_executed_repr);

            serverReplStreamSwitch2Xsync(
                    parsed->xcontinue.replid,parsed->xcontinue.reploff,
                    parsed->xcontinue.master_uuid,
                    reply_executed,parsed->xcontinue.gtid_lost,RS_UPDATE_DOWN,
                    "slave psync=>xcontinue");
            serverReplStreamResurrectCreate(conn,-1,
                    parsed->xcontinue.replid,parsed->xcontinue.reploff);
            result = PSYNC_CONTINUE;

            gtidSetFree(reply_executed);
        } else {
            /* psync => fullresync, psync => continue handled by redis. */
            if (parsed->type == SYNC_REPLY_FULLRESYNC) {
                server.gtid_sync_stat[GTID_SYNC_PSYNC_FULLRESYNC]++;
            }
            if (parsed->type == SYNC_REPLY_CONTINUE) {
                server.gtid_sync_stat[GTID_SYNC_PSYNC_CONTINUE]++;
            }
            goto by_redis;
        }
    } else {
        serverAssert(!server.cached_master && !server.master);
        if (parsed->type == SYNC_REPLY_FULLRESYNC) {
            /* XSYNC => FULLRESYNC */
            server.gtid_sync_stat[GTID_SYNC_XSYNC_FULLRESYNC]++;
            serverLog(LL_NOTICE,
                    "[xsync] repl mode switch: xsync => psync (fullresync)");
            goto by_redis;
        } else if (parsed->type == SYNC_REPLY_CONTINUE) {
            /* XSYNC => CONTINUE */
            server.gtid_sync_stat[GTID_SYNC_XSYNC_CONTINUE]++;
            serverLog(LL_NOTICE,
                    "[xsync] repl mode switch: xsync => psync (continue)");
            sds replid = parsed->pcontinue.replid;
            long long reploff = parsed->pcontinue.reploff;
            serverReplStreamSwitch2Psync(replid,reploff,RS_UPDATE_DOWN,
                    "slave xsync=>continue");
            serverReplStreamResurrectCreate(conn,-1,replid,reploff);
            result = PSYNC_CONTINUE;
        } else if (parsed->type == SYNC_REPLY_XFULLRESYNC) {
            /* XSYNC => XFULLRESYNC */
            server.gtid_sync_stat[GTID_SYNC_XSYNC_XFULLRESYNC]++;
            serverLog(LL_NOTICE,"[xsync] XFullResync from master: %s.", reply);
            parsedSyncReplySetupGtidInital(parsed);
            result = PSYNC_FULLRESYNC;
        } else if (parsed->type == SYNC_REPLY_XCONTINUE) {
            /* XSYNC => XCONTINUE */
            server.gtid_sync_stat[GTID_SYNC_XSYNC_XCONTINUE]++;
            serverLog(LL_NOTICE,
                    "[xsync] Successful partial xsync with master: %s", reply);

            gtidSet *gtid_cont = parsed->xcontinue.gtid_cont;
            gtidSet *gtid_slost = NULL, *gtid_slave = NULL, *gtid_mlost = NULL;
            sds gtid_cont_repr, gtid_slave_repr, gtid_slost_repr;

            gtid_slave = serverGtidSetGet("[xsync]");
            gtid_slave_repr = gtidSetDump(gtid_slave);
            gtid_slost = gtidSetDup(gtid_cont);
            gtidSetDiff(gtid_slost,gtid_slave);
            gtid_cont_repr = gtidSetDump(gtid_cont);
            gtid_slost_repr = gtidSetDump(gtid_slost);
            serverLog(LL_NOTICE, "[xsync] gtid.set-slost(%s) = "
                    "gtid.set-continue(%s) - gtid.set-slave(%s)",
                    gtid_slost_repr,gtid_cont_repr,gtid_slave_repr);
            
            if (server.gtid_gaplog_enabled) {
                gtid_mlost = gtidSetDup(gtid_slave);
                gtidSetDiff(gtid_mlost, gtid_cont);
                sds gtid_mlost_repr = gtidSetDump(gtid_mlost);
                serverLog(LL_WARNING, "[gaplog] gtid_mlost = %s, count = %d",
                        gtid_mlost_repr, (int)gtidSetCount(gtid_mlost));
                sdsfree(gtid_mlost_repr);
                if (gtidSetCount(gtid_mlost) > 0) {
                    saveGapLogFromGtidSet(gtid_mlost);
                }
                gtidSetFree(gtid_mlost);
            }

            /* Update gtid lost, master.uuid or replid/reploff. */
            serverReplStreamUpdateXsync(gtid_slost,NULL,
                    parsed->xcontinue.master_uuid,
                    parsed->xcontinue.replid,parsed->xcontinue.reploff);
            serverReplStreamResurrectCreate(conn,-1,
                    parsed->xcontinue.replid,parsed->xcontinue.reploff);

            sdsfree(gtid_cont_repr), sdsfree(gtid_slave_repr),
                sdsfree(gtid_slost_repr);
            gtidSetFree(gtid_slost), gtidSetFree(gtid_slave),
            result = PSYNC_CONTINUE;
        }
    }

end:
    sdsfree(reply);
    parsedSyncReplyFree(parsed);
    return result;

by_redis:
    parsedSyncReplyFree(parsed);
    return PSYNC_BY_REDIS;
}

#ifdef REDIS_TEST
int gtidTest(int argc, char **argv, int accurate) {
    UNUSED(argc), UNUSED(argv), UNUSED(accurate);
    server.proto_max_bulk_len = 512LL*1024*1024;

    int error = 0;
    server.maxmemory_policy = MAXMEMORY_FLAG_LFU;
    server.gtid_xsync_max_gap = 10000;
    if (!server.logfile) server.logfile = zstrdup(CONFIG_DEFAULT_LOGFILE);

    TEST("gtid - parse xsync request") {
        syncRequest *request = syncRequestNew();
        robj *optargv[4];
        robj *gtidset = createStringObject("A:1-100,B", 9);
        robj *uuid_interested = createStringObject("*",1);
        optargv[0] = createStringObject("GTID.LOST",9);
        optargv[1] = createStringObject("A:81-100", 8);
        optargv[2] = createStringObject("MAXGAP",6);
        optargv[3] = createStringObject("10000", 5);
        masterParseXsyncRequest(request,uuid_interested,gtidset,4,optargv);
        test_assert(request->mode == REPL_MODE_XSYNC);
        test_assert(request->x.maxgap == 10000);
        test_assert(gtidSetCount(request->x.gtid_slave) == 100);
        test_assert(gtidSetCount(request->x.gtid_lost) == 20);
        decrRefCount(optargv[0]);
        decrRefCount(optargv[1]);
        decrRefCount(optargv[2]);
        decrRefCount(optargv[3]);
        decrRefCount(gtidset);
        decrRefCount(uuid_interested);
        syncRequestFree(request);
    }

    TEST("gtid - parse invalid xsync request") {
        syncRequest *request = syncRequestNew();
        robj *gtidset = createStringObject("hello:world", 11);
        robj *uuid_interested = createStringObject("*",1);
        masterParseXsyncRequest(request,uuid_interested,gtidset,0,NULL);
        test_assert(request->mode == REPL_MODE_UNSET);
        test_assert(!strcmp(request->i.msg, "invalid gtid.set hello:world"));
        decrRefCount(gtidset);
        decrRefCount(uuid_interested);
        syncRequestFree(request);
    }

    TEST("gtid - locate sync request") {
        syncLocateResult slr[1];

        /* {(xsync:2000), (psync:1000)}*/
        server.prev_repl_mode->from = 1000, server.prev_repl_mode->mode = REPL_MODE_PSYNC;
        server.repl_mode->from = 2000, server.repl_mode->mode = REPL_MODE_XSYNC;

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_XSYNC,3000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_CUR);
        test_assert(slr->locate_mode == REPL_MODE_XSYNC);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_XSYNC,2000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_CUR);
        test_assert(slr->locate_mode == REPL_MODE_XSYNC);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_PSYNC,2000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_SWITCH);
        test_assert(slr->locate_mode == REPL_MODE_XSYNC);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_XSYNC,1000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_INVALID);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_PSYNC,1000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_PREV);
        test_assert(slr->locate_mode == REPL_MODE_PSYNC);
        test_assert(slr->p.limit == 1000);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_XSYNC,500,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_INVALID);
        syncLocateResultDeinit(slr);

        /* {(psync:2000), (xsync:1000)}*/
        server.prev_repl_mode->from = 1000, server.prev_repl_mode->mode = REPL_MODE_XSYNC;
        server.repl_mode->from = 2000, server.repl_mode->mode = REPL_MODE_PSYNC;

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_PSYNC,3000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_CUR);
        test_assert(slr->locate_mode == REPL_MODE_PSYNC);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_PSYNC,2000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_CUR);
        test_assert(slr->locate_mode == REPL_MODE_PSYNC);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_XSYNC,2000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_SWITCH);
        test_assert(slr->locate_mode == REPL_MODE_PSYNC);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_PSYNC,1000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_INVALID);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_XSYNC,1000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_PREV);
        test_assert(slr->locate_mode == REPL_MODE_XSYNC);
        test_assert(slr->p.limit == 1000);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_XSYNC,500,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_INVALID);
        syncLocateResultDeinit(slr);

        /* {(xsync:2000), {?:1000}}*/
        server.prev_repl_mode->from = 1000, server.prev_repl_mode->mode = REPL_MODE_UNSET;
        server.repl_mode->from = 2000, server.repl_mode->mode = REPL_MODE_XSYNC;

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_XSYNC,3000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_CUR);
        test_assert(slr->locate_mode == REPL_MODE_XSYNC);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_XSYNC,2000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_CUR);
        test_assert(slr->locate_mode == REPL_MODE_XSYNC);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_PSYNC,2000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_INVALID);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_PSYNC,1000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_INVALID);
        syncLocateResultDeinit(slr);

        /* {(psync:1000), (xsync:1000)}*/
        server.prev_repl_mode->from = 1000, server.prev_repl_mode->mode = REPL_MODE_XSYNC;
        server.repl_mode->from = 1000, server.repl_mode->mode = REPL_MODE_PSYNC;

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_PSYNC,1000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_CUR);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_XSYNC,1000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_SWITCH);
        syncLocateResultDeinit(slr);

        syncLocateResultInit(slr);
        locateServerReplMode(REPL_MODE_XSYNC,900,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_INVALID);
        syncLocateResultDeinit(slr);
    }

    TEST("gtid - parse sync reply") {
        parsedSyncReply *parsed;
        sds reply, replid = sdsnew("0123456789012345678901234567890123456789"),
            master_uuid = sdsnew("A");
        gtidSet *gtid_cont = gtidSetDecode("A:1,B:2",7);
        gtidSet *gtid_lost = gtidSetDecode("C:3",3);

        reply = sdsnew("+FULLRESYNC invalid_replid 10");
        parsed = parsedSyncReplyNew();
        parseSyncReplyFullresync(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_INVALID);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+FULLRESYNC 0123456789012345678901234567890123456789 1000");
        parsed = parsedSyncReplyNew();
        parseSyncReplyFullresync(reply,parsed);
        test_assert(parsed->type = SYNC_REPLY_FULLRESYNC);
        test_assert(sdscmp(parsed->fullresync.replid, replid) == 0);
        test_assert(parsed->fullresync.reploff == 1000);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+CONTINUE");
        parsed = parsedSyncReplyNew();
        parseSyncReplyContinue(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_CONTINUE);
        test_assert(parsed->pcontinue.replid == NULL);
        test_assert(parsed->pcontinue.reploff_is_set == 0);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+CONTINUE 0123456789012345678901234567890123456789");
        parsed = parsedSyncReplyNew();
        parseSyncReplyContinue(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_CONTINUE);
        test_assert(sdscmp(parsed->pcontinue.replid,replid) == 0);
        test_assert(parsed->pcontinue.reploff_is_set == 0);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+CONTINUE 0123456789012345678901234567890123456789 1234");
        parsed = parsedSyncReplyNew();
        parseSyncReplyContinue(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_CONTINUE);
        test_assert(sdscmp(parsed->pcontinue.replid,replid) == 0);
        test_assert(parsed->pcontinue.reploff_is_set == 1);
        test_assert(parsed->pcontinue.reploff == 1234);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+CONTINUE invalid_replid 1234");
        parsed = parsedSyncReplyNew();
        parseSyncReplyContinue(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_INVALID);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+XFULLRESYNC");
        parsed = parsedSyncReplyNew();
        parseSyncReplyXfullresync(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_INVALID);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+XFULLRESYNC GTID.LOST \"\" MASTER.UUID master-uuid");
        parsed = parsedSyncReplyNew();
        parseSyncReplyXfullresync(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_INVALID);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+XFULLRESYNC GTID.LOST \"\" MASTER.UUID master-uuid REPLID 0123456789012345678901234567890123456789 REPLOFF 1234 FOO BAR\r\n");
        parsed = parsedSyncReplyNew();
        parseSyncReplyXfullresync(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_XFULLRESYNC);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+XCONTINUE");
        parsed = parsedSyncReplyNew();
        parseSyncReplyXcontinue(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_INVALID);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+XCONTINUE GTID.SET A:1,B:2 MASTER.UUID A");
        parsed = parsedSyncReplyNew();
        parseSyncReplyXcontinue(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_INVALID);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+XCONTINUE REPLID 0123456789012345678901234567890123456789 REPLOFF 1234 GTID.SET A:1,B:2 MASTER.UUID A FOO BAR");
        parsed = parsedSyncReplyNew();
        parseSyncReplyXcontinue(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_XCONTINUE);
        test_assert(gtidSetEqual(parsed->xcontinue.gtid_cont,gtid_cont));
        test_assert(gtidSetCount(parsed->xcontinue.gtid_lost) == 0);
        test_assert(sdscmp(parsed->xcontinue.master_uuid,master_uuid) == 0);
        test_assert(sdscmp(parsed->xcontinue.replid,replid) == 0);
        test_assert(parsed->xcontinue.reploff == 1234);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        reply = sdsnew("+XCONTINUE REPLID 0123456789012345678901234567890123456789 REPLOFF 1234 GTID.SET A:1,B:2 GTID.LOST C:3 MASTER.UUID A FOO BAR");
        parsed = parsedSyncReplyNew();
        parseSyncReplyXcontinue(reply,parsed);
        test_assert(parsed->type == SYNC_REPLY_XCONTINUE);
        test_assert(gtidSetEqual(parsed->xcontinue.gtid_cont,gtid_cont));
        test_assert(gtidSetEqual(parsed->xcontinue.gtid_lost,gtid_lost));
        test_assert(sdscmp(parsed->xcontinue.master_uuid,master_uuid) == 0);
        test_assert(sdscmp(parsed->xcontinue.replid,replid) == 0);
        test_assert(parsed->xcontinue.reploff == 1234);
        parsedSyncReplyFree(parsed), sdsfree(reply);

        sdsfree(replid), sdsfree(master_uuid);
        gtidSetFree(gtid_cont);
        gtidSetFree(gtid_lost);
    }

    TEST("gtid - gapLog key new and release") {
        sds key = sdsnew("testkey");
        sds *subkeys = zmalloc(sizeof(sds) * 2);
        subkeys[0] = sdsnew("field1");
        subkeys[1] = sdsnew("field2");
        /* test hash */
        gtidGaplogKey *gk = gtidGaplogKeyNew(0, OBJ_HASH, key, subkeys, 2);
        test_assert(gk != NULL);
        test_assert(gk->dbid == 0);
        test_assert(gk->key_type == OBJ_HASH);
        test_assert(gk->subkeys_count == 2);
        test_assert(sdslen(gk->key) == 7);   /* "testkey" */
        test_assert(sdslen(gk->subkeys[0]) == 6); /* "field1" */
        test_assert(sdslen(gk->subkeys[1]) == 6); /* "field2" */

        /* test string */
        sds key2 = sdsnew("strkey");
        gtidGaplogKey *gk2 = gtidGaplogKeyNew(1, OBJ_STRING, key2, NULL, 0);
        test_assert(gk2 != NULL);
        test_assert(gk2->dbid == 1);
        test_assert(gk2->key_type == OBJ_STRING);
        test_assert(gk2->subkeys_count == 0);
        test_assert(gk2->subkeys == NULL);
        test_assert(sdslen(gk2->key) == 6);  /* "strkey" */

        /* release */
        gtidGaplogKeyRelease(gk);
        gtidGaplogKeyRelease(gk2);

        /* release NULL*/
        gtidGaplogKeyRelease(NULL);
    }

    TEST("gtid - gapLog keys builder, build and release") {
        /* test builder */
        gtidGaplogKeysBuilder builder = GTID_GAPLOG_KEYS_BUILDER_INIT;

        /* add 2 keys */
        gtidGaplogKeysPrepareBuilder(&builder, 2);

        sds key1 = sdsnew("key_one");
        sds key2 = sdsnew("key_two");
        builder.keys_infos[builder.numkeys++] =
            gtidGaplogKeyNew(0, OBJ_STRING, key1, NULL, 0);
        builder.keys_infos[builder.numkeys++] =
            gtidGaplogKeyNew(1, OBJ_LIST, key2, NULL, 0);

        test_assert(builder.numkeys == 2);

        /* builder => gtidGaplogKeys */
        gtidGaplogKeys *keys = gtidGaplogKeysBuild(&builder);
        test_assert(keys != NULL);
        test_assert(keys->size == 2);
        test_assert(builder.numkeys == 0); /* builder clean */

        test_assert(keys->keys[0]->dbid == 0);
        test_assert(keys->keys[0]->key_type == OBJ_STRING);
        test_assert(sdslen(keys->keys[0]->key) == 7); /* "key_one" */
        test_assert(keys->keys[1]->dbid == 1);
        test_assert(keys->keys[1]->key_type == OBJ_LIST);
        test_assert(sdslen(keys->keys[1]->key) == 7); /* "key_two" */

        /* release keys */
        gtidGaplogKeysRelease(keys);
        gtidGaplogDeinitKeysBuilder(&builder);
    }

    TEST("gtid - gapLog new, reset and release") {

        gtidGaplog *gap_log = gtidGaplogNew();
        test_assert(gap_log != NULL);
        test_assert(gap_log->size == 0);
        test_assert(gap_log->data != NULL);
        test_assert(gap_log->history != NULL);
        test_assert(dictSize(gap_log->data) == 0);
        test_assert(listLength(gap_log->history) == 0);

        /* reset */
        gtidGaplogReset(gap_log);
        test_assert(gap_log->size == 0);
        test_assert(dictSize(gap_log->data) == 0);
        test_assert(listLength(gap_log->history) == 0);

        /* relase */
        gtidGaplogRelease(gap_log);
        zfree(gap_log);
    }

    TEST("gtid - gapLog data insert and iterate") {
        gtidGaplog *gap_log = gtidGaplogNew();
        sds uuid = sdsnew("uuid-test");

        /* add keys (gno=1) */
        {
            gtidGaplogKeysBuilder builder = GTID_GAPLOG_KEYS_BUILDER_INIT;
            gtidGaplogKeysPrepareBuilder(&builder, 1);
            sds k = sdsnew("key_a");
            builder.keys_infos[builder.numkeys++] =
                gtidGaplogKeyNew(0, OBJ_STRING, k, NULL, 0);
            gtidGaplogKeys *keys = gtidGaplogKeysBuild(&builder);
            gtidGaplogDeinitKeysBuilder(&builder);
            
            int ret = gtidGaplogInsert(gap_log, uuid, 1, keys);
            test_assert(ret == 1);
        }

        /* add keys (gno=5) */
        {
            gtidGaplogKeysBuilder builder = GTID_GAPLOG_KEYS_BUILDER_INIT;
            gtidGaplogKeysPrepareBuilder(&builder, 1);
            sds k = sdsnew("hashkey");
            sds *subs = zmalloc(sizeof(sds) * 1);
            subs[0] = sdsnew("field_a");
            builder.keys_infos[builder.numkeys++] =
                gtidGaplogKeyNew(0, OBJ_HASH, k, subs, 1);
            gtidGaplogKeys *keys = gtidGaplogKeysBuild(&builder);
            gtidGaplogDeinitKeysBuilder(&builder);            
            int ret = gtidGaplogInsert(gap_log, uuid, 5, keys);
            test_assert(ret == 1);
        }

        test_assert(gtidGaplogSize(gap_log) == 2);

        listNode *ln = listFirst(gap_log->history);
        test_assert(ln != NULL);
        uuidSet *us = (uuidSet*)listNodeValue(ln);
        test_assert(us != NULL);
        
        sds stored_uuid = sdsnewlen(us->uuid, us->uuid_len);
        dictEntry *de = dictFind(gap_log->data, stored_uuid);
        sdsfree(stored_uuid);
        test_assert(de != NULL);
        skiplist *sl = dictGetVal(de);
        gtidGaplogDataIterator iter;
        gtidGaplogDataInitIterator(&iter, sl, 1);
        gno_t gno = gtidGaplogDataGetGno(&iter);
        test_assert(gno == 1);
        gtidGaplogKeys *k1 = gtidGaplogDataNext(&iter);
        test_assert(k1 != NULL);
        test_assert(k1->size == 1);
        test_assert(k1->keys[0]->key_type == OBJ_STRING);
        test_assert(sdslen(k1->keys[0]->key) == 5); /* "key_a" */

        /* 2 node */
        gno = gtidGaplogDataGetGno(&iter);
        test_assert(gno == 5);
        gtidGaplogKeys *k2 = gtidGaplogDataNext(&iter);
        test_assert(k2 != NULL);
        test_assert(k2->size == 1);
        test_assert(k2->keys[0]->key_type == OBJ_HASH);
        test_assert(k2->keys[0]->subkeys_count == 1);
        test_assert(sdslen(k2->keys[0]->subkeys[0]) == 7); /* "field_a" */

        gtidGaplogKeys *k3 = gtidGaplogDataNext(&iter);
        test_assert(k3 == NULL);

        gtidGaplogDeinitDataIterator(&iter);

        gtidGaplogDataInitIterator(&iter, sl, 3);
        gno = gtidGaplogDataGetGno(&iter);
        test_assert(gno == 5);
        gtidGaplogKeys *k_mid = gtidGaplogDataNext(&iter);
        test_assert(k_mid != NULL);
        test_assert(sdslen(k_mid->keys[0]->key) == 7); /* "hashkey" */
        gtidGaplogDeinitDataIterator(&iter);

        gtidGaplogDataInitIterator(&iter, sl, 10);
        gno = gtidGaplogDataGetGno(&iter);
        test_assert(gno == -1); /* not find node */
        gtidGaplogKeys *k_empty = gtidGaplogDataNext(&iter);
        test_assert(k_empty == NULL);
        gtidGaplogDeinitDataIterator(&iter);

        sdsfree(uuid);
        gtidGaplogRelease(gap_log);
    }

    TEST("gtid - gapLog history iterator") {
        
        gtidGaplog *gap_log = gtidGaplogNew();

        /* add uuid-1: [1-3, 10-12] */
        uuidSet *us1 = uuidSetNew("uuid-1", 6);
        uuidSetAdd(us1, 1, 3);
        uuidSetAdd(us1, 10, 12);
        listAddNodeTail(gap_log->history, us1);

        /* add uuid-2: [100-101] */
        uuidSet *us2 = uuidSetNew("uuid-2", 6);
        uuidSetAdd(us2, 100, 101);
        listAddNodeTail(gap_log->history, us2);

        /*  history iterator */
        gtidGaplogHistoryIterator iter;
        gtidGaplogInitHistoryIterator(&iter, gap_log, 0);

        const char *uuid;
        size_t uuid_len;

        /* uuid-1: gno=1 */
        gno_t gno = gtidGaplogHistoryNext(&iter, &uuid, &uuid_len);
        test_assert(gno == 1);
        test_assert(uuid_len == 6);
        test_assert(memcmp(uuid, "uuid-1", 6) == 0);

        /* uuid-1: gno=2 */
        gno = gtidGaplogHistoryNext(&iter, &uuid, &uuid_len);
        test_assert(gno == 2);
        test_assert(memcmp(uuid, "uuid-1", 6) == 0);

        /* uuid-1: gno=3 */
        gno = gtidGaplogHistoryNext(&iter, &uuid, &uuid_len);
        test_assert(gno == 3);
        test_assert(memcmp(uuid, "uuid-1", 6) == 0);

        /* uuid-1: gno=10（ interval） */
        gno = gtidGaplogHistoryNext(&iter, &uuid, &uuid_len);
        test_assert(gno == 10);
        test_assert(memcmp(uuid, "uuid-1", 6) == 0);

        /* uuid-1: gno=11 */
        gno = gtidGaplogHistoryNext(&iter, &uuid, &uuid_len);
        test_assert(gno == 11);

        /* uuid-1: gno=12 */
        gno = gtidGaplogHistoryNext(&iter, &uuid, &uuid_len);
        test_assert(gno == 12);

        /* uuid-2: gno=100（ uuidSet） */
        gno = gtidGaplogHistoryNext(&iter, &uuid, &uuid_len);
        test_assert(gno == 100);
        test_assert(uuid_len == 6);
        test_assert(memcmp(uuid, "uuid-2", 6) == 0);

        /* uuid-2: gno=101 */
        gno = gtidGaplogHistoryNext(&iter, &uuid, &uuid_len);
        test_assert(gno == 101);

        gno = gtidGaplogHistoryNext(&iter, &uuid, &uuid_len);
        test_assert(gno == 0);
        test_assert(uuid == NULL);

        gtidGaplogDeinitHistoryIterator(&iter);

        gtidGaplogInitHistoryIterator(&iter, gap_log, 4);
        gno = gtidGaplogHistoryNext(&iter, &uuid, &uuid_len);
        test_assert(gno == 11);
        test_assert(memcmp(uuid, "uuid-1", 6) == 0);
        gtidGaplogDeinitHistoryIterator(&iter);

        gtidGaplogRelease(gap_log);
        zfree(gap_log);
    }

    TEST("gtid - gapLog history iterator Seek by index") {
        gtidGaplog *gap_log = gtidGaplogNew();

        uuidSet *us1 = uuidSetNew("uuid-1", 6);
        uuidSetAdd(us1, 1, 3);
        uuidSetAdd(us1, 10, 12);
        listAddNodeTail(gap_log->history, us1);

        uuidSet *us2 = uuidSetNew("uuid-2", 6);
        uuidSetAdd(us2, 100, 101);
        listAddNodeTail(gap_log->history, us2);

        const char *uuid;
        size_t uuid_len;

        gtidGaplogHistoryIterator iter;
        gtidGaplogInitHistoryIterator(&iter, gap_log, 0);
        gno_t gno = gtidGaplogHistoryNext(&iter, &uuid, &uuid_len);
        test_assert(gno == 1);
        test_assert(memcmp(uuid, "uuid-1", 6) == 0);
        gtidGaplogDeinitHistoryIterator(&iter);

        gtidGaplogInitHistoryIterator(&iter, gap_log, 4);
        gno = gtidGaplogHistoryNext(&iter, &uuid, &uuid_len);
        test_assert(gno == 11);
        test_assert(memcmp(uuid, "uuid-1", 6) == 0);
        gtidGaplogDeinitHistoryIterator(&iter);

        gtidGaplogInitHistoryIterator(&iter, gap_log, 6);
        gno = gtidGaplogHistoryNext(&iter, &uuid, &uuid_len);
        test_assert(gno == 100);
        test_assert(memcmp(uuid, "uuid-2", 6) == 0);
        gtidGaplogDeinitHistoryIterator(&iter);

        gtidGaplogInitHistoryIterator(&iter, gap_log, 7);
        gno = gtidGaplogHistoryNext(&iter, &uuid, &uuid_len);
        test_assert(gno == 101);
        test_assert(memcmp(uuid, "uuid-2", 6) == 0);
        gtidGaplogDeinitHistoryIterator(&iter);

        gtidGaplogInitHistoryIterator(&iter, gap_log, 8);
        gno = gtidGaplogHistoryNext(&iter, &uuid, &uuid_len);
        test_assert(gno == 0);
        test_assert(uuid == NULL);
        gtidGaplogDeinitHistoryIterator(&iter);

        gtidGaplogInitHistoryIterator(&iter, gap_log, 0);
        gno = gtidGaplogHistoryNext(&iter, &uuid, &uuid_len);
        test_assert(gno == 1);
        gtidGaplogDeinitHistoryIterator(&iter);

        gtidGaplogRelease(gap_log);
        zfree(gap_log);
    }

    TEST("gtid - gapLog trim basic") {
        gtidGaplog *gap_log = gtidGaplogNew();
        sds uuid = sdsnew("uuid-A");

        /* add key (gno=2) */
        {
            gtidGaplogKeysBuilder builder = GTID_GAPLOG_KEYS_BUILDER_INIT;
            gtidGaplogKeysPrepareBuilder(&builder, 2);
            sds k1 = sdsnew("trimkey1");
            sds k2 = sdsnew("trimkey2");
            builder.keys_infos[builder.numkeys++] =
                gtidGaplogKeyNew(0, OBJ_STRING, k1, NULL, 0);
            builder.keys_infos[builder.numkeys++] =
                gtidGaplogKeyNew(0, OBJ_STRING, k2, NULL, 0);
            gtidGaplogKeys *keys = gtidGaplogKeysBuild(&builder);
            gtidGaplogDeinitKeysBuilder(&builder);

            int ret = gtidGaplogInsert(gap_log, uuid, 2, keys);
            test_assert(ret == 1);
        }

        test_assert(gtidGaplogSize(gap_log) == 1);

        int trimmed = gtidGaplogTrim(gap_log, 1);
        test_assert(trimmed == 1);
        test_assert(gtidGaplogSize(gap_log) == 0);
        test_assert(listLength(gap_log->history) == 0);

        sdsfree(uuid);
        gtidGaplogRelease(gap_log);
    }

    TEST("gtid - readBacklogIterator init and deinit") {
        readBacklogIterator it;
        readBacklogIteratorInit(&it);
        test_assert(it.backlog == -1);
        test_assert(it.mock.querybuf != NULL);
        test_assert(sdslen(it.mock.querybuf) == 0);
        test_assert(it.mock.qb_pos == 0);

        readBacklogIteratorDeinit(&it);
        test_assert(it.backlog == -1);  
        test_assert(it.mock.querybuf == NULL);
    }

    TEST("gtid - readBacklogIterator SeekTo basic (init + no-op)") {
        readBacklogIterator it;
        readBacklogIteratorInit(&it);
        test_assert(it.backlog == -1);

        readBacklogIteratorSeekTo(&it, 100);
        test_assert(it.backlog == 100);
        test_assert(sdslen(it.mock.querybuf) == 0);
        test_assert(it.mock.qb_pos == 0);

        /* no-op seek：offset == cur */
        readBacklogIteratorSeekTo(&it, 100);
        test_assert(it.backlog == 100);
        test_assert(it.mock.qb_pos == 0);

        readBacklogIteratorDeinit(&it);
    }

    TEST("gtid - readBacklogIterator SeekTo forward within buffer") {
        readBacklogIterator it;
        readBacklogIteratorInit(&it);

        readBacklogIteratorSeekTo(&it, 0);
        it.backlog = 1200;
        it.mock.querybuf = sdscatlen(it.mock.querybuf, "x", 200);  
        it.mock.qb_pos = 0;


        readBacklogIteratorSeekTo(&it, 1050);
        test_assert(it.backlog == 1200);
        test_assert(it.mock.qb_pos == 0);  
        test_assert(sdslen(it.mock.querybuf) == 150);  

        readBacklogIteratorDeinit(&it);
    }

    TEST("gtid - readBacklogIterator SeekTo forward past buffer (clear+seek)") {
        readBacklogIterator it;
        readBacklogIteratorInit(&it);

        it.backlog = 1000;
        it.mock.querybuf = sdscatlen(it.mock.querybuf, "x", 100);  /* [1000, 1100) */
        it.mock.qb_pos = 0;

        readBacklogIteratorSeekTo(&it, 1200);
        test_assert(it.backlog == 1200);
        test_assert(sdslen(it.mock.querybuf) == 0);
        test_assert(it.mock.qb_pos == 0);

        readBacklogIteratorDeinit(&it);
    }

    TEST("gtid - readBacklogIterator SeekTo rewind (clear+seek)") {
        readBacklogIterator it;
        readBacklogIteratorInit(&it);

        it.backlog = 1000;
        it.mock.querybuf = sdscatlen(it.mock.querybuf, "x", 200);
        it.mock.qb_pos = 0;

        readBacklogIteratorSeekTo(&it, 500);
        test_assert(it.backlog == 500);
        test_assert(sdslen(it.mock.querybuf) == 0);
        test_assert(it.mock.qb_pos == 0);

        readBacklogIteratorDeinit(&it);
    }

    TEST("gtid - readBacklogIterator ParseNext single command") {
        server.repl_backlog_size = 2048;
        /* Set up backlog with a single SET command */
        if (server.repl_backlog == NULL) ctrip_createReplicationBacklog();
        sds cmd = sdsnew("*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
        feedReplicationBacklog(cmd, sdslen(cmd));
        long long start_off = gtidGetBacklogOffset() ;

        readBacklogIterator it;
        readBacklogIteratorInit(&it);
        readBacklogIteratorSeekTo(&it, 1);

        robj **argv;
        int argc;
        ssize_t consumed = readBacklogIteratorParseNext(&it, &argv, &argc);
        test_assert(consumed > 0);
        test_assert(argc == 3);
        test_assert(!strcasecmp(argv[0]->ptr, "set"));
        test_assert(!strcasecmp(argv[1]->ptr, "key"));
        test_assert(!strcasecmp(argv[2]->ptr, "value"));
        test_assert(it.backlog == start_off + consumed);

        readBacklogIteratorDeinit(&it);
        sdsfree(cmd);
    }

    return error;
}

#endif


