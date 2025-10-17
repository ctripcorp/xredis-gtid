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


int isGtidExecCommand(client* c) {
    return c->cmd->proc == gtidCommand && c->argc > GTID_COMMAN_ARGC &&
        strcasecmp(c->argv[GTID_COMMAN_ARGC]->ptr, "exec") == 0;
}

sds gtidSetDump(gtidSet *gtid_set) {
    size_t gtidlen, estlen = gtidSetEstimatedEncodeBufferSize(gtid_set);
    sds gtidrepr = sdsnewlen(NULL,estlen);
    gtidlen = gtidSetEncode(gtidrepr,estlen,gtid_set);
    sdssetlen(gtidrepr,gtidlen);
    return gtidrepr;
}

sds gtidSetQuoteIfEmpty(sds gtid_repr) {
    if (sdslen(gtid_repr) == 0) return sdscat(gtid_repr, "\"\"");
    else return gtid_repr;
}

gtidSet *serverGtidSetGet(char *log_prefix) {
    gtidSet *gtid_master = gtidSetDup(server.gtid_executed);
    gtidSetMerge(gtid_master,server.gtid_lost);
    if (log_prefix != NULL) {
        sds gtid_master_repr = gtidSetDump(gtid_master);
        sds gtid_executed_repr = gtidSetDump(server.gtid_executed);
        sds gtid_lost_repr = gtidSetDump(server.gtid_lost);
        serverLog(LL_NOTICE,
                "%s gtid.set(%s) = gtid.set-executed(%s) + gtid.set-lost(%s)",
                log_prefix, gtid_master_repr, gtid_executed_repr,gtid_lost_repr);
        sdsfree(gtid_master_repr), sdsfree(gtid_executed_repr);
        sdsfree(gtid_lost_repr);
    }
    return gtid_master;
}

int serverGtidSetContains(char *uuid, size_t uuid_len, gno_t gno) {
    return gtidSetContains(server.gtid_executed,uuid,uuid_len,gno) ||
        gtidSetContains(server.gtid_lost,uuid,uuid_len,gno);
}

static void serverGtidSetCurrrentUuidSetUpdateNextGno() {
    gno_t curnext, lostnext;
    curnext = gtidSetCurrentUuidSetNext(server.gtid_executed,0);
    lostnext = gtidSetNext(server.gtid_lost,server.uuid,server.uuid_len,0);
    if (curnext < lostnext) {
        gtidSetCurrentUuidSetSetNextGno(server.gtid_executed, lostnext);
        serverLog(LL_NOTICE,
                "[gtid] current uuid next gno updated from %lld to %lld",
                curnext, lostnext);
    }
}

void serverGtidSetResetExecuted(gtidSet *gtid_executed) {
    if (server.gtid_executed) gtidSetFree(server.gtid_executed);
    server.gtid_executed = gtid_executed;
    gtidSetCurrentUuidSetUpdate(server.gtid_executed,server.uuid,
            server.uuid_len);
    serverGtidSetCurrrentUuidSetUpdateNextGno();
}

void serverGtidSetResetLost(gtidSet *gtid_lost) {
    if (server.gtid_lost) gtidSetFree(server.gtid_lost);
    server.gtid_lost = gtid_lost;
    serverGtidSetCurrrentUuidSetUpdateNextGno();
}

void serverGtidSetAddLost(gtidSet *delta_lost) {
    gtidSetMerge(server.gtid_lost,delta_lost);
    serverGtidSetCurrrentUuidSetUpdateNextGno();
}

void serverGtidSetRemoveLost(gtidSet *delta_lost) {
    gtidSetDiff(server.gtid_lost,delta_lost);
}

void serverGtidSetAddExecuted(gtidSet *delta_executed) {
    gtidSetMerge(server.gtid_lost,delta_executed);
}

void serverGtidSetRemoveExecuted(gtidSet *delta_executed) {
    gtidSetMerge(server.gtid_lost,delta_executed);
    serverGtidSetCurrrentUuidSetUpdateNextGno();
}

/**
 * @brief
 *      1. gtid A:1 {db} set k v
 *      2. gtid A:1 {db} exec
 *          a. fail (clean queue)
 */
void gtidCommand(client *c) {
    sds gtid = c->argv[1]->ptr;
    long long gno = 0;
    size_t uuid_len = 0;
    char* uuid = uuidGnoDecode(gtid, sdslen(gtid), &gno, &uuid_len);
    if (uuid == NULL) {
        addReplyErrorFormat(c,"gtid format error:%s", gtid);
        return;
    }
    int id = 0;
    if (getIntFromObjectOrReply(c, c->argv[2], &id, NULL) != C_OK)
        return;

    if (selectDb(c, id) == C_ERR) {
        addReplyError(c,"DB index is out of range");
        return;
    }
    if (serverGtidSetContains(uuid,uuid_len,gno)) {
        sds args = sdsempty();
        for (int i=1, len=GTID_COMMAN_ARGC + 1; i < len && sdslen(args) < 256; i++) {
            args = sdscatprintf(args, "`%.*s`, ", 128-(int)sdslen(args), (char*)c->argv[i]->ptr);
        }
        addReplyStatusFormat(c,"gtid command already executed, %s",args);
        if(isGtidExecCommand(c)) {
            discardTransaction(c);
        }
        server.gtid_ignored_cmd_count++;
        if (server.masterhost) {
            serverLog(LL_WARNING, "[CRITICAL] gtid command already execute, %s", args);
        } else {
            serverLog(LL_NOTICE, "gtid command already execute, %s", args);
        }
        sdsfree(args);
        return;
    }

    int orig_argc = c->argc;
    c->argc -= GTID_COMMAN_ARGC;

    robj** orig_argv = c->argv;
    robj** newargv = zmalloc(sizeof(struct robj*) * c->argc);
    for(int i = 0; i < c->argc; i++) {
        newargv[i] = c->argv[i + GTID_COMMAN_ARGC];
        incrRefCount(newargv[i]);
    }
    c->argv = newargv;

    struct redisCommand* orig_cmd = c->cmd, *orig_lastcmd = c->lastcmd;
    c->cmd = c->lastcmd = lookupCommand(c->argv, c->argc);
    if (!c->cmd) {
        sds args = sdsempty();
        int i;
        for (i=1; i < c->argc && sdslen(args) < 128; i++)
            args = sdscatprintf(args, "`%.*s`, ", 128-(int)sdslen(args), (char*)c->argv[i]->ptr);
        serverLog(LL_WARNING, "unknown command `%s`, with args beginning with: %s",
            (char*)c->argv[0]->ptr, args);
        rejectCommandFormat(c,"unknown command `%s`, with args beginning with: %s",
            (char*)c->argv[0]->ptr, args);
        sdsfree(args);
        goto end;
    } else if ((c->cmd->arity > 0 && c->cmd->arity != c->argc) ||
               (c->argc < -c->cmd->arity)) {
        serverLog(LL_WARNING,"wrong number of arguments for '%s' command",
            c->cmd->fullname);
        rejectCommandFormat(c,"wrong number of arguments for '%s' command",
            c->cmd->fullname);
        goto end;
    }

    c->cmd->proc(c);
    serverAssert(gtidSetAdd(server.gtid_executed, uuid, uuid_len, gno, gno));
    server.gtid_executed_cmd_count++;

end:
    for(int i = 0; i < c->argc; i++) {
        decrRefCount(c->argv[i]);
    }
    zfree(c->argv);
    c->argv = orig_argv;
    c->argc = orig_argc;
    c->cmd = orig_cmd;
    c->lastcmd = orig_lastcmd;
}

const char *getMasterUuid(size_t *puuid_len) {
    const char *uuid;
    size_t uuid_len;
    if (server.masterhost) {
        uuid = server.master_uuid;
        uuid_len = server.master_uuid_len;
    } else {
        uuid = server.uuid;
        uuid_len = server.uuid_len;
    }
    if (puuid_len) *puuid_len = uuid_len;
    return uuid;
}

void setMasterUuid(const char *master_uuid, size_t master_uuid_len) {
    if (master_uuid_len > CONFIG_RUN_ID_SIZE) {
        serverLog(LL_WARNING, "Trim master_uuid (%ld => %d): %s",
                master_uuid_len, CONFIG_RUN_ID_SIZE, master_uuid);
        master_uuid_len = CONFIG_RUN_ID_SIZE;
    }
    memcpy(server.master_uuid,master_uuid,master_uuid_len);
    server.master_uuid[master_uuid_len] = '\0';
    server.master_uuid_len = master_uuid_len;
}

void clearMasterUuid() {
    memset(server.master_uuid,'0',CONFIG_RUN_ID_SIZE);
    server.master_uuid[CONFIG_RUN_ID_SIZE] = 0;
    server.master_uuid_len = CONFIG_RUN_ID_SIZE;
}

static inline int isWrongTypeErrorReply(const char *s, size_t len) {
    const char *swaperrmsg = "Swap failed (code=-206)";
    const char *wterrmsg = "WRONGTYPE";
    size_t swaplen = 23, wtlen = 9;

    if (len > 0 && s[0] == '-') {
        s++;
        len--;
    }

    if ( (len >= swaplen && !memcmp(s,swaperrmsg,swaplen)) ||
            (len >= wtlen && !memcmp(s,wterrmsg,wtlen)) )
        return 1;
    else
        return 0;
}

void ctrip_afterErrorReply(client *c, const char *s, size_t len, int flags) {
    afterErrorReply(c,s,len, flags);
    if (server.repl_mode->mode != REPL_MODE_XSYNC) return;
    /* Replica sending wrong type error to master indicates data
     * inconsistent, * force fullresync to fix it. */
    if (getClientType(c) == CLIENT_TYPE_MASTER &&
            isWrongTypeErrorReply(s,len)) {
        server.gtid_xsync_fullresync_indicator++;
    }
}

/* Note: uuid interested is effective only once */
void xsyncUuidInterestedSet(const char *uuid) {
    server.gtid_uuid_interested = uuid;
}

const char *xsyncUuidInterestedGet(void) {
    const char *uuid_interested = server.gtid_uuid_interested;
    return uuid_interested;
}

void xsyncUuidInterestedInit() {
    xsyncUuidInterestedSet(GTID_XSYNC_UUID_INTERESTED_DEFAULT);
}

void forceXsyncFullResync() {
    xsyncUuidInterestedSet(GTID_XSYNC_UUID_INTERESTED_FULLRESYNC);
}

void forceXsyncFullResyncIfNeeded() {
    static long long prev_xsync_fullresync_indicator;

    serverAssert(server.repl_mode->mode == REPL_MODE_XSYNC);
    serverAssert(prev_xsync_fullresync_indicator <=
            server.gtid_xsync_fullresync_indicator);

    if (prev_xsync_fullresync_indicator ==
            server.gtid_xsync_fullresync_indicator) return;

    serverLog(LL_WARNING,
            "[gtid] Force xsync fullresync (indicator %lld => %lld)",
            prev_xsync_fullresync_indicator,
            server.gtid_xsync_fullresync_indicator);

    prev_xsync_fullresync_indicator =
        server.gtid_xsync_fullresync_indicator;

    forceXsyncFullResync();
    if (server.master) freeClient(server.master);
}

void xsyncReplicationCron() {
    forceXsyncFullResyncIfNeeded();
}

sds genGtidInfoString(sds info) {
    gtidSet *gtid_set;
    gtidStat executed_stat, lost_stat;

    gtidSetGetStat(server.gtid_executed, &executed_stat);
    gtidSetGetStat(server.gtid_lost, &lost_stat);

    gtid_set = serverGtidSetGet(NULL);
    sds gtid_set_repr = gtidSetDump(gtid_set);
    gtidSetFree(gtid_set);

    sds gtid_executed_repr = gtidSetDump(server.gtid_executed);
    sds gtid_lost_repr = gtidSetDump(server.gtid_lost);

    const char *master_uuid = getMasterUuid(NULL);
    info  = sdscatprintf(info,
        "gtid_uuid:%s\r\n"
        "gtid_master_uuid:%s\r\n"
        "gtid_set:%s\r\n"
        "gtid_executed:%s\r\n"
        "gtid_executed_gno_count:%lld\r\n"
        "gtid_executed_used_memory:%lu\r\n"
        "gtid_lost:%s\r\n"
        "gtid_lost_gno_count:%lld\r\n"
        "gtid_lost_used_memory:%lu\r\n"
        "gtid_repl_mode:%s\r\n"
        "gtid_repl_from:%lld\r\n"
        "gtid_repl_detail:%s\r\n"
        "gtid_prev_repl_mode:%s\r\n"
        "gtid_prev_repl_from:%lld\r\n"
        "gtid_prev_repl_detail:%s\r\n"
        "gtid_reploff_delta:%lld\r\n"
        "gtid_master_repl_offset:%lld\r\n"
        "gtid_uuid_interested:%s\r\n"
        "gtid_xsync_fullresync_indicator:%lld\r\n"
        "gtid_executed_cmd_count:%lld\r\n"
        "gtid_ignored_cmd_count:%lld\r\n",
        server.uuid,
        master_uuid,
        gtid_set_repr,
        gtid_executed_repr,
        executed_stat.gno_count,
        executed_stat.used_memory,
        gtid_lost_repr,
        lost_stat.gno_count,
        lost_stat.used_memory,
        replModeName(server.repl_mode->mode),
        server.repl_mode->from,
        serverReplModeGetCurDetail(),
        replModeName(server.prev_repl_mode->mode),
        server.prev_repl_mode->from,
        serverReplModeGetPrevDetail(),
        server.gtid_reploff_delta,
        server.master_repl_offset,
        server.gtid_uuid_interested,
        server.gtid_xsync_fullresync_indicator,
        server.gtid_executed_cmd_count,
        server.gtid_ignored_cmd_count
    );

    sdsfree(gtid_set_repr);
    sdsfree(gtid_executed_repr);
    sdsfree(gtid_lost_repr);

    info = sdscatprintf(info,"gtid_sync_stat:");
    for (int i = 0; i < GTID_SYNC_TYPES; i++) {
        long long count = server.gtid_sync_stat[i];
        if (i == 0) {
            info = sdscatprintf(info,"%s=%lld",gtidSyncTypeName(i),count);
        } else {
            info = sdscatprintf(info,",%s=%lld",gtidSyncTypeName(i),count);
        }
    }
    info = sdscatprintf(info,"\r\n");

    return info;
}

sds catGtidStatString(sds info, gtidStat *stat) {
    return sdscatprintf(info,
            "uuid_count:%ld,used_memory:%ld,gap_count:%ld,gno_count:%lld",
            stat->uuid_count, stat->used_memory, stat->gap_count,
            stat->gno_count);
}

long long copyReplicationBacklogLimited(char *buf, long long offset, long long limit);

static gtidSet *findGtidSetOrReply(client *c, int type_argc) {
    gtidSet *gtid_set = NULL;
    if (!strcasecmp(c->argv[type_argc]->ptr,"executed")) {
        gtid_set = server.gtid_executed;
    } else if (!strcasecmp(c->argv[type_argc]->ptr,"lost")) {
        gtid_set = server.gtid_lost;
    } else {
        addReplyError(c, "invalid gtid.set type");
    }
    return gtid_set;
}

/* gtid manage command */
void gtidxCommand(client *c) {
    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        const char *help[] = {
            "LIST EXECUTED|LOST [<uuid>]",
            "    List gtid or uuid(if specified) gaps.",
            "STAT EXECUTED|LOST [<uuid>]",
            "    Show gtid or uuid(if specified) stat.",
            "ADD EXECUTED|LOST <uuid> <start_gno> <end_gno>",
            "    Add gno range to gtidset.",
            "REMOVE EXECUTED|LOST <uuid> <start_gno> <end_gno>",
            "    Remove gno range from gtidset.",
            "SEQ",
            "    List gtid.seq.",
            "SEQ GTID.SET",
            "    Get gtid.set of gtid seq index.",
            "SEQ LOCATE <gtid.set>",
            "    Locate xsync continue position",
            "UUID-INTRESTED SET <*|?>",
            "    SET uuid.interested to * or ?",
            NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(c->argv[1]->ptr,"list")
            && (c->argc == 3 || c->argc == 4)) {
        gtidSet *gtid_set;
        char *buf;
        size_t maxlen, len;

        if ((gtid_set = findGtidSetOrReply(c,2)) == NULL) return;

        if (c->argc == 3) {
            maxlen = gtidSetEstimatedEncodeBufferSize(gtid_set);
            buf = zcalloc(maxlen);
            len = gtidSetEncode(buf, maxlen, gtid_set);
            addReplyBulkCBuffer(c, buf, len);
            zfree(buf);
        } else {
            sds uuid = c->argv[3]->ptr;
            uuidSet *uuid_set = gtidSetFind(gtid_set, uuid, sdslen(uuid));
            if (uuid_set) {
                maxlen = uuidSetEstimatedEncodeBufferSize(uuid_set);
                buf = zcalloc(maxlen);
                len = uuidSetEncode(buf, maxlen, uuid_set);
                addReplyBulkCBuffer(c, buf, len);
                zfree(buf);
            } else {
                addReplyErrorFormat(c, "uuid not found:%s", uuid);
            }
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"stat")
            && (c->argc == 3 || c->argc == 4)) {
        gtidSet *gtid_set;
        gtidStat stat;
        sds stat_str;

        if ((gtid_set = findGtidSetOrReply(c,2)) == NULL) return;

        if (c->argc == 3) {
            gtidSetGetStat(gtid_set, &stat);
            stat_str = sdscatprintf(sdsempty(), "all:");
            stat_str = catGtidStatString(stat_str, &stat);
            addReplyBulkSds(c, stat_str);
        } else {
            sds uuid = c->argv[3]->ptr, stat_str;
            uuidSet *uuid_set = gtidSetFind(gtid_set, uuid, sdslen(uuid));
            if (uuid_set == NULL) {
                addReplyErrorFormat(c, "uuid not found:%s", uuid);
            } else {
                uuidSetGetStat(uuid_set, &stat);
                stat_str = sdscatprintf(sdsempty(), "%s:", uuid);
                stat_str = catGtidStatString(stat_str, &stat);
                addReplyBulkSds(c, stat_str);
            }
        }

    } else if (!strcasecmp(c->argv[1]->ptr,"add") && c->argc == 6) {
        gtidSet *gtid_set;
        sds uuid = c->argv[3]->ptr;
        gno_t start_gno, end_gno, added;

        if ((gtid_set = findGtidSetOrReply(c,2)) == NULL) return;
        if (getLongLongFromObjectOrReply(c, c->argv[4], &start_gno, NULL)
                != C_OK) return;
        if (getLongLongFromObjectOrReply(c, c->argv[5], &end_gno, NULL)
                != C_OK) return;
        added = gtidSetAdd(gtid_set,uuid,sdslen(uuid),start_gno,end_gno);
        serverGtidSetCurrrentUuidSetUpdateNextGno();
        sds gtid_repr = gtidSetDump(gtid_set);
        serverLog(LL_NOTICE,"Add gtid-%s %s %lld %lld result: %s",
                (char*)c->argv[2]->ptr,uuid,start_gno,end_gno,gtid_repr);
        sdsfree(gtid_repr);
        addReplyLongLong(c,added);
    } else if (!strcasecmp(c->argv[1]->ptr,"remove") && c->argc == 6) {
        gtidSet *gtid_set;
        sds uuid = c->argv[3]->ptr;
        gno_t start_gno, end_gno, removed;

        if ((gtid_set = findGtidSetOrReply(c,2)) == NULL) return;
        if (getLongLongFromObjectOrReply(c, c->argv[4], &start_gno, NULL)
                != C_OK) return;
        if (getLongLongFromObjectOrReply(c, c->argv[5], &end_gno, NULL)
                != C_OK) return;
        removed = gtidSetRemove(gtid_set,uuid,sdslen(uuid),start_gno,end_gno);
        if (gtid_set == server.gtid_executed) {
            /* update current uuid set because it might be removed. */
            gtidSetCurrentUuidSetUpdate(server.gtid_executed,server.uuid,
                    server.uuid_len);
        }
        sds gtid_repr = gtidSetDump(gtid_set);
        serverLog(LL_NOTICE,"Remove gtid-%s %s %lld %lld result: %s",
                (char*)c->argv[2]->ptr,uuid,start_gno,end_gno,gtid_repr);
        sdsfree(gtid_repr);
        addReplyLongLong(c,removed);
    } else if (!strcasecmp(c->argv[1]->ptr,"seq") && c->argc >= 2) {
        if (c->argc == 2) {
            if (server.gtid_seq) {
                size_t maxlen = gtidSeqEstimatedEncodeBufferSize(server.gtid_seq);
                char *buf = zmalloc(maxlen);
                size_t len = gtidSeqEncode(buf,maxlen,server.gtid_seq);
                addReplyBulkCBuffer(c,buf,len);
            } else {
                addReplyError(c, "gtid seq not exists");
            }
        } else if (!strcasecmp(c->argv[2]->ptr,"locate") && c->argc >= 4) {
            if (server.gtid_seq) {
                long long blmaxlen = 128;
                char *blbuf;
                size_t bllen = 0;
                sds gtidrepr = c->argv[3]->ptr;
                gtidSet *gtid_req = NULL,*gtid_cont = NULL;
                gtid_req = gtidSetDecode(gtidrepr,sdslen(gtidrepr));
                if (c->argc > 4) getLongLongFromObject(c->argv[4],&blmaxlen);
                serverLog(LL_WARNING, "blmaxlen=%lld", blmaxlen);
                blbuf = zcalloc(blmaxlen);
                long long offset = gtidSeqXsync(server.gtid_seq,gtid_req,&gtid_cont);
                size_t maxlen = gtidSetEstimatedEncodeBufferSize(gtid_cont);
                char *buf = zmalloc(maxlen);
                size_t len = gtidSetEncode(buf,maxlen,gtid_cont);
                addReplyArrayLen(c,3);
                addReplyBulkLongLong(c,offset);
                addReplyBulkCBuffer(c,buf,len);
                if (offset >= 0) {
                    bllen = copyReplicationBacklogLimited(blbuf,offset,blmaxlen-1);
                    addReplyBulkCBuffer(c,blbuf,bllen);
                } else {
                    addReplyNull(c);
                }
                zfree(buf);
                zfree(blbuf);
                gtidSetFree(gtid_cont);
                gtidSetFree(gtid_req);
            } else {
                addReplyError(c, "gtid seq not exists");
            }
        } else if (!strcasecmp(c->argv[2]->ptr,"gtid.set") && c->argc == 3) {
            if (server.gtid_seq) {
                gtidSegment *seg;
                gtidSet *gtid_set = gtidSetNew();
                for (seg = server.gtid_seq->firstseg; seg != NULL;
                        seg = seg->next) {
                    gtidSetAdd(gtid_set,seg->uuid,seg->uuid_len,
                            seg->base_gno+seg->tgno,seg->base_gno+seg->ngno-1);
                }
                size_t maxlen = gtidSetEstimatedEncodeBufferSize(gtid_set);
                char *buf = zmalloc(maxlen);
                size_t len = gtidSetEncode(buf,maxlen,gtid_set);
                addReplyBulkCBuffer(c,buf,len);
                zfree(buf);
                gtidSetFree(gtid_set);
            } else {
                addReplyError(c, "gtid seq not exists");
            }
        } else {
            addReplyError(c,"Syntax error");
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"uuid-interested") && c->argc >= 3) {
        if (!strcasecmp(c->argv[2]->ptr, "set") && c->argc == 4) {
            if (!strcasecmp(c->argv[3]->ptr, GTID_XSYNC_UUID_INTERESTED_DEFAULT)){
                xsyncUuidInterestedSet(GTID_XSYNC_UUID_INTERESTED_DEFAULT);
                addReply(c,shared.ok);
            } else if (!strcasecmp(c->argv[3]->ptr, GTID_XSYNC_UUID_INTERESTED_FULLRESYNC)) {
                xsyncUuidInterestedSet(GTID_XSYNC_UUID_INTERESTED_FULLRESYNC);
                addReply(c,shared.ok);
            } else {
                addReplyError(c,"Invalid uuid-interested");
            }
        } else {
            addReplyError(c,"Syntax error");
        }
    } else {
        addReplySubcommandSyntaxError(c);
    }

}


