#include "server.h"
#include <gtid.h>
#include <ctype.h>

#define GTID_COMMAN_ARGC 3

sds gtidSetDump(gtidSet *gtid_set) {
    size_t gtidlen, estlen = gtidSetEstimatedEncodeBufferSize(gtid_set);
    sds gtidrepr = sdsnewlen(NULL,estlen);
    gtidlen = gtidSetEncode(gtidrepr,estlen,gtid_set);
    sdssetlen(gtidrepr,gtidlen);
    return gtidrepr;
}

uuidSet* gtidSetFind(gtidSet* gtid_set, const char* uuid, size_t uuid_len);
/* return true if set1 and set2 has common uuid */
int gtidSetRelated(gtidSet *set1, gtidSet *set2) {
    uuidSet *uuid_set = set2->header;
    while (uuid_set) {
        if (gtidSetFind(set1,uuid_set->uuid,uuid_set->uuid_len)) return 1;
        uuid_set = uuid_set->next;
    }
    return 0;
}

int gtidSetEqual(gtidSet *set1, gtidSet *set2) {
    gtidSet *set;
    gno_t count;

    set = gtidSetDup(set1);
    gtidSetDiff(set,set2);
    count = gtidSetCount(set);
    gtidSetFree(set);

    if (count) return 0;

    set = gtidSetDup(set2);
    gtidSetDiff(set,set1);
    count = gtidSetCount(set);
    gtidSetFree(set);

    return count ? 0 : 1;
}


void resetServerReplMode(int mode) {
    replModeInit(server.prev_repl_mode);
    server.repl_mode->mode = mode;
    server.repl_mode->from = server.master_repl_offset;
    serverLog(LL_NOTICE, "[gtid] Reset repl mode to %s:%lld",
            replModeName(mode),server.master_repl_offset);
}

void shiftServerReplMode(int mode) {
    serverAssert(mode != REPL_MODE_UNSET);
    if (server.repl_mode->mode == mode) return;
    if (server.repl_mode->from != server.master_repl_offset) {
        server.prev_repl_mode->from = server.repl_mode->from;
        server.prev_repl_mode->mode = server.repl_mode->mode;
        serverLog(LL_NOTICE,"[gtid] Save repl mode %s:%lld",
                replModeName(server.repl_mode->mode),server.repl_mode->from);
    }
    server.repl_mode->from = server.master_repl_offset;
    server.repl_mode->mode = mode;
    serverLog(LL_NOTICE, "[gtid] Switch to repl mode %s:%lld",
            replModeName(mode),server.master_repl_offset);
    serverLog(LL_WARNING, "[gtid] Disconnect slaves to sync repl mode.");
    disconnectSlaves();
}

int locateServerReplMode(long long offset, int *switch_mode) {
    serverAssert(server.repl_mode->mode != REPL_MODE_UNSET);
    if (offset >= server.repl_mode->from) {
        *switch_mode = offset == server.repl_mode->from;
        return server.repl_mode->mode;
    }
    serverAssert(server.prev_repl_mode->mode != REPL_MODE_UNSET);
    serverAssert(offset >= server.prev_repl_mode->from);
    *switch_mode = offset == server.repl_mode->from;
    return server.prev_repl_mode->mode;
}

int isGtidExecCommand(client* c) {
    return c->cmd->proc == gtidCommand && c->argc > GTID_COMMAN_ARGC &&
        strcasecmp(c->argv[GTID_COMMAN_ARGC]->ptr, "exec") == 0;
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
    int uuid_len = 0;
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
    uuidSet* uuid_set = gtidSetFind(server.gtid_executed, uuid, uuid_len);
    if(uuid_set != NULL && uuidSetContains(uuid_set, gno)) {
        sds args = sdsempty();
        for (int i=1, len=GTID_COMMAN_ARGC + 1; i < len && sdslen(args) < 128; i++) {
            args = sdscatprintf(args, "`%.*s`, ", 128-(int)sdslen(args), (char*)c->argv[i]->ptr);
        }
        addReplyErrorFormat(c,"gtid command is executed, %s",args);
        sdsfree(args);
        if(isGtidExecCommand(c)) {
            discardTransaction(c);
        }
        server.gtid_ignored_cmd_count++;
        return;
    }
    int argc = c->argc;
    robj** argv = c->argv;

    struct redisCommand* cmd = c->cmd;
    robj** newargv = zmalloc(sizeof(struct robj*) * argc);
    int gtid_argc = 3;
    if(strncmp(c->argv[3]->ptr,"/*", 2) == 0) {
        gtid_argc = 4;
    }
    c->argc = argc - gtid_argc;
    for(int i = 0; i < c->argc; i++) {
        newargv[i] = argv[i + gtid_argc];
        incrRefCount(newargv[i]);
    }
    c->argv = newargv;
    c->cmd = c->lastcmd = lookupCommand(c->argv[0]->ptr);
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
        return;
    } else if ((c->cmd->arity > 0 && c->cmd->arity != c->argc) ||
               (c->argc < -c->cmd->arity)) {
        serverLog(LL_WARNING,"wrong number of arguments for '%s' command",
            c->cmd->name);
        rejectCommandFormat(c,"wrong number of arguments for '%s' command",
            c->cmd->name);
        goto end;
        return;
    }
    c->cmd->proc(c);
    for(int i = 0; i < c->argc; i++) {
        decrRefCount(c->argv[i]);
    }
    zfree(c->argv);
    c->argv = NULL;
    int result = 0;
    if(uuid_set != NULL) {
        result = uuidSetAdd(uuid_set, gno, gno);
    } else {
        result = gtidSetAdd(server.gtid_executed, uuid, uuid_len, gno, gno);
    }
    server.gtid_executed_cmd_count++;
    serverAssert(result > 0);
end:
    c->argc = argc;
    c->argv = argv;
    c->cmd = cmd;
}

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
    long long offset, value;
    size_t bufmaxlen, buflen;
    int argc, dbid, uuid_len = server.uuid_len;
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
            pargs->orig_cmd->proc == gtidCommand) {
        gtid_repr = pargs->orig_argv[1]->ptr;
        uuid = uuidGnoDecode(gtid_repr,sdslen(gtid_repr),&gno,&uuid_len);
        getLongLongFromObject(pargs->orig_argv[2],&value);
    }

    /* Rewrite args to gtid... if needed */
    if (server.masterhost != NULL ||
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

/* gtid expireat command append buffer */
sds catAppendOnlyGtidExpireAtCommand(sds buf, robj* gtid, robj* dbid,
        robj* comment, struct redisCommand *cmd,  robj *key, robj *seconds) {
    long long when;
    robj *argv[7];

    /* Make sure we can use strtoll */
    seconds = getDecodedObject(seconds);
    when = strtoll(seconds->ptr,NULL,10);
    /* Convert argument into milliseconds for EXPIRE, SETEX, EXPIREAT */
    if (cmd->proc == expireCommand || cmd->proc == setexCommand ||
        cmd->proc == expireatCommand)
    {
        when *= 1000;
    }
    /* Convert into absolute time for EXPIRE, PEXPIRE, SETEX, PSETEX */
    if (cmd->proc == expireCommand || cmd->proc == pexpireCommand ||
        cmd->proc == setexCommand || cmd->proc == psetexCommand)
    {
        when += mstime();
    }
    decrRefCount(seconds);
    int index = 0, time_index = 0;
    argv[index++] = shared.gtid;
    argv[index++] = gtid;
    argv[index++] = dbid;
    if(comment != NULL) {
        argv[index++] = comment;
    }
    argv[index++] = shared.pexpireat;
    argv[index++] = key;
    time_index = index;
    argv[index++] = createStringObjectFromLongLong(when);
    buf = catAppendOnlyGenericCommand(buf, index, argv);
    decrRefCount(argv[time_index]);
    return buf;
}

/**
 * @brief check feedAppendOnlyFile for more details
 *        gtid expire => gtid expireat
 *        gtid setex =>  set  + gtid expireat
 *        gtid set(ex) => set + gtid expireat
 */
void feedAppendOnlyFileGtid(struct redisCommand *_cmd, int dictid, robj **argv, int argc) {
    struct redisCommand *cmd;
    sds buf = sdsempty();

    serverAssert(_cmd == server.gtidCommand);
    cmd = lookupCommand(argv[GTID_COMMAN_ARGC]->ptr);

    if (dictid != server.aof_selected_db) {
        char seldb[64];

        snprintf(seldb,sizeof(seldb),"%d",dictid);
        buf = sdscatprintf(buf,"*2\r\n$6\r\nSELECT\r\n$%lu\r\n%s\r\n",
            (unsigned long)strlen(seldb),seldb);
        server.aof_selected_db = dictid;
    }

    if (cmd->proc == expireCommand || cmd->proc == pexpireCommand ||
            cmd->proc == expireatCommand) {
        buf = catAppendOnlyGtidExpireAtCommand(buf,argv[1],argv[2],NULL,cmd,
                argv[GTID_COMMAN_ARGC+1],argv[GTID_COMMAN_ARGC+2]);
    } else if (cmd->proc == setCommand && argc > 3 + GTID_COMMAN_ARGC) {
        robj *pxarg = NULL;
        if (!strcasecmp(argv[3 + GTID_COMMAN_ARGC]->ptr, "px")) {
            pxarg = argv[4 + GTID_COMMAN_ARGC];
        }
        if (pxarg) {
            robj *millisecond = getDecodedObject(pxarg);
            long long when = strtoll(millisecond->ptr,NULL,10);
            when += mstime();

            decrRefCount(millisecond);

            robj *newargs[5 + GTID_COMMAN_ARGC];
            for(int i = 0, len = 3 + GTID_COMMAN_ARGC; i < len; i++) {
                newargs[i] = argv[i];
            }
            newargs[3 + GTID_COMMAN_ARGC] = shared.pxat;
            newargs[4 + GTID_COMMAN_ARGC] = createStringObjectFromLongLong(when);
            buf = catAppendOnlyGenericCommand(buf,5 + GTID_COMMAN_ARGC,newargs);
            decrRefCount(newargs[4 + GTID_COMMAN_ARGC]);
        } else {
            buf = catAppendOnlyGenericCommand(buf,argc,argv);
        }
    } else {
        buf = catAppendOnlyGenericCommand(buf,argc,argv);
    }

    if (server.aof_state == AOF_ON)
        server.aof_buf = sdscatlen(server.aof_buf,buf,sdslen(buf));

    if (server.child_type == CHILD_TYPE_AOF)
        aofRewriteBufferAppend((unsigned char*)buf,sdslen(buf));

    sdsfree(buf);
}

void ctrip_feedAppendOnlyFile(struct redisCommand *cmd, int dictid,
        robj **argv, int argc) {
    if (cmd == server.gtidCommand)
        feedAppendOnlyFileGtid(cmd,dictid,argv,argc);
    else
        feedAppendOnlyFile(cmd,dictid,argv,argc);
}

#define GTID_AUX_REPL_MODE    "gtid-repl-mode"
#define GTID_AUX_EXECUTED     "gtid-executed"
#define GTID_AUX_LOST         "gtid-lost"

int rdbSaveGtidInfoAuxFields(rio* rdb) {
    char *repl_mode = (char*)replModeName(server.repl_mode->mode);
    sds gtid_executed_repr = gtidSetDump(server.gtid_executed);
    sds gtid_lost_repr = gtidSetDump(server.gtid_lost);

    if (rdbSaveAuxField(rdb, GTID_AUX_REPL_MODE, strlen(GTID_AUX_REPL_MODE),
                repl_mode, strlen(repl_mode)) == 0) {
        goto err;
    }

    if (rdbSaveAuxField(rdb, GTID_AUX_EXECUTED,
                strlen(GTID_AUX_EXECUTED),
                gtid_executed_repr, sdslen(gtid_executed_repr)) == -1) {
        goto err;
    }

    if (rdbSaveAuxField(rdb, GTID_AUX_LOST,
                strlen(GTID_AUX_LOST),
                gtid_lost_repr, sdslen(gtid_lost_repr)) == -1) {
        goto err;
    }

    sdsfree(gtid_executed_repr);
    sdsfree(gtid_lost_repr);
    return 1;

err:
    sdsfree(gtid_executed_repr);
    sdsfree(gtid_lost_repr);
    return -1;
}

int LoadGtidInfoAuxFields(robj* key, robj* val) {
    if (!strcasecmp(key->ptr, GTID_AUX_REPL_MODE)) {
        if (!strcasecmp(val->ptr, "xsync")) {
            resetServerReplMode(REPL_MODE_XSYNC);
        } else if (!strcasecmp(val->ptr, "psync")) {
            resetServerReplMode(REPL_MODE_PSYNC);
        } else {
            serverLog(LL_WARNING,
                    "[gtid] Parsed invalid repl mode: %s",(sds)val->ptr);
            resetServerReplMode(REPL_MODE_PSYNC);
        }
        return 1;
    } else if (!strcasecmp(key->ptr, GTID_AUX_EXECUTED)) {
        gtidSetFree(server.gtid_executed);
        server.gtid_executed = gtidSetDecode(val->ptr, sdslen(val->ptr));
        gtidSetCurrentUuidSetUpdate(server.gtid_executed,server.uuid,
                server.uuid_len);
        return 1;
    } else if (!strcasecmp(key->ptr, GTID_AUX_LOST)) {
        gtidSet *gtid_lost;
        gtidSetFree(server.gtid_lost);
        if (!(gtid_lost = gtidSetDecode(val->ptr, sdslen(val->ptr))))
            gtid_lost = gtidSetNew();
        server.gtid_lost = gtid_lost;
        return 1;
    }
    return 0;
}


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

void ctrip_replicationFeedSlaves(list *slaves, int dictid, robj **argv,
        int argc, const char *uuid, size_t uuid_len, gno_t gno, long long offset) {
    int touch_index = uuid != NULL && gno >= GTID_GNO_INITIAL && server.gtid_seq
        && server.masterhost == NULL;
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

int ctrip_replicationSetupSlaveForFullResync(client *slave, long long offset) {
    const char *xfullresync = "+XFULLRESYNC\r\n";
    int xfulllen = strlen(xfullresync);

    if (server.repl_mode->mode != REPL_MODE_XSYNC)
        return replicationSetupSlaveForFullResync(slave, offset);

    slave->psync_initial_offset = offset;
    slave->replstate = SLAVE_STATE_WAIT_BGSAVE_END;
    /* We are going to accumulate the incremental changes for this
     * slave as well. Set slaveseldb to -1 in order to force to re-emit
     * a SELECT statement in the replication stream. */
    server.slaveseldb = -1;

    /* Don't send this reply to slaves that approached us with
     * the old SYNC command. */
    if (!(slave->flags & CLIENT_PRE_PSYNC)) {
        if (connWrite(slave->conn,xfullresync,xfulllen) != xfulllen) {
            freeClientAsync(slave);
            return C_ERR;
        }
    }
    return C_OK;
}

/* Check adReplyReplicationBacklog for more details */
long long addReplyReplicationBacklogLimited(client *c, long long offset,
        long long limit) {
    long long added = 0, j, skip, len;
    serverAssert(limit >= 0 && offset >= server.repl_backlog_off);
    if (server.repl_backlog_histlen == 0) return 0;
    skip = offset - server.repl_backlog_off;
    j = (server.repl_backlog_idx +
        (server.repl_backlog_size-server.repl_backlog_histlen)) %
        server.repl_backlog_size;
    j = (j + skip) % server.repl_backlog_size;
    len = server.repl_backlog_histlen - skip;
    len = MIN(len,limit); /* limit bytes to copy */
    while(len) {
        long long thislen =
            ((server.repl_backlog_size - j) < len) ?
            (server.repl_backlog_size - j) : len;
        addReplySds(c,sdsnewlen(server.repl_backlog + j, thislen));
        len -= thislen;
        j = 0;
        added += thislen;
    }

    return added;
}

/* Check adReplyReplicationBacklog for more details */
long long copyReplicationBacklogLimited(char *buf, long long limit,
        long long offset) {
    long long added = 0, j, skip, len;
    serverAssert(limit >= 0 && offset >= server.repl_backlog_off);
    if (server.repl_backlog_histlen == 0) return 0;
    skip = offset - server.repl_backlog_off;
    j = (server.repl_backlog_idx +
        (server.repl_backlog_size-server.repl_backlog_histlen)) %
        server.repl_backlog_size;
    j = (j + skip) % server.repl_backlog_size;
    len = server.repl_backlog_histlen - skip;
    len = MIN(len,limit); /* limit bytes to copy */
    while(len) {
        long long thislen =
            ((server.repl_backlog_size - j) < len) ?
            (server.repl_backlog_size - j) : len;
        memcpy(buf + added,server.repl_backlog + j, thislen);
        added += thislen;
        buf[added] = '\0';
        len -= thislen;
        j = 0;
    }

    return added;
}

/* like addReplyReplicationBacklog, but send backlog untill repl mode
 * switch, return 1 if mode switched. */
int ctrip_addReplyReplicationBacklog(client *c, long long offset,
        long long *added) {
    long long limit;
    if (server.prev_repl_mode->mode == REPL_MODE_UNSET ||
            server.repl_mode->mode == REPL_MODE_UNSET ||
            offset >= server.repl_mode->from) {
        *added = addReplyReplicationBacklog(c,offset);
        return 0;
    }
    limit = server.repl_mode->from - server.repl_backlog_off;
    serverAssert(limit >= 0);
    addReplyReplicationBacklogLimited(c,offset,limit);
    return 1;
}

/* see masterTryPartialResynchronization for more details. */
void masterSetupPartialSynchronization(client *c, long long psync_offset,
        char *buf, int buflen) {
    int disconect;
    long long psync_len;

    c->flags |= CLIENT_SLAVE;
    c->replstate = SLAVE_STATE_ONLINE;
    c->repl_ack_time = server.unixtime;
    c->repl_put_online_on_ack = 0;
    listAddNodeTail(server.slaves,c);

    if (connWrite(c->conn,buf,buflen) != buflen) {
        freeClientAsync(c);
        return;
    }

    disconect = ctrip_addReplyReplicationBacklog(c,psync_offset,&psync_len);
    serverLog(LL_NOTICE,
        "[xsync] Sending %lld bytes of backlog starting from offset %lld.",
        psync_len, psync_offset);

    if (disconect) {
        serverLog(LL_NOTICE,
                "[xsync] Disconnect slave %s to notify repl mode switched.",
                replicationGetSlaveName(c));
        freeClientAsync(c);
        return;
    }

    /* Note that we don't need to set the selected DB at server.slaveseldb
     * to -1 to force the master to emit SELECT, since the slave already
     * has this state from the previous connection with the master. */

    refreshGoodSlavesCount();

    /* Fire the replica change modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICA_CHANGE,
                          REDISMODULE_SUBEVENT_REPLICA_CHANGE_ONLINE,
                          NULL);
}

void masterSetupPartialXsynchronization(client *c, long long psync_offset,
        gtidSet *gtid_cont) {
    char *buf;
    int buflen;
    sds gtidrepr = gtidSetDump(gtid_cont);
    buflen = sdslen(gtidrepr)+256;
    buf = zcalloc(buflen);
    buflen = snprintf(buf,buflen,
            "+XCONTINUE GTID.SET %.*s MASTER.SID %.*s\r\n",
             (int)sdslen(gtidrepr),gtidrepr,
             (int)server.uuid_len,server.uuid);
    masterSetupPartialSynchronization(c,psync_offset,buf,buflen);
    zfree(buf);
    zfree(gtidrepr);
}

void masterSetupPartialResynchronization(client *c, long long psync_offset) {
    char buf[128];
    int buflen;
    /* serverAssert(psync_offset >= server.repl_backlog_off && psync_offset <= server.master_repl_offset); */
    buflen = snprintf(buf,sizeof(buf),"+CONTINUE %s %lld\r\n",
             server.master_replid, psync_offset);
    masterSetupPartialSynchronization(c,psync_offset,buf,buflen);
}

int ctrip_masterTryPartialResynchronization(client *c) {
    long long psync_offset, max_gap = 0;
    gtidSet *gtid_slave = NULL, *gtid_cont = NULL, *gtid_xsync = NULL,
            *gtid_gap = NULL, *gtid_mlost = NULL, *gtid_slost = NULL;
    const char *mode = c->argv[0]->ptr;
    int request_mode, locate_mode, switch_mode, result;

    /* get paritial sync request offset:
     *   psync: request offset specified in command
     *   xsync: offset of continue point */
    if (!strcasecmp(mode,"psync")) {
        request_mode = REPL_MODE_PSYNC;
        if (getLongLongFromObjectOrReply(c,c->argv[2],&psync_offset,NULL)
                != C_OK) {
            serverLog(LL_WARNING,
                    "[psync] Partial sync from %s rejected:offset %s invalid",
                    replicationGetSlaveName(c),(sds)c->argv[2]->ptr);
            goto need_full_resync;
        }
    } else if (!strcasecmp(mode,"xsync")) {
        sds gtid_repr = c->argv[2]->ptr;
        request_mode = REPL_MODE_XSYNC;

        for (int i = 3; i+1 < c->argc; i += 2) {
            if (!strcasecmp(c->argv[i]->ptr,"maxgap")) {
                if (getLongLongFromObjectOrReply(c,c->argv[i+1],&max_gap,NULL)
                        != C_OK) {
                    serverLog(LL_WARNING,
                            "[xsync] Ignored invalid maxgap option: %s",
                            (sds)c->argv[i+1]->ptr);
                }
            } else {
                serverLog(LL_WARNING, "[xsync] Ignored unknown xsync option: %s",
                        (sds)c->argv[i]->ptr);
            }
        }

        serverLog(LL_NOTICE, "[xsync] Repica asked for partial xsync with "
                "gtid.set=%s, maxgap=%lld", gtid_repr, max_gap);

        gtid_slave = gtidSetDecode(gtid_repr,sdslen(gtid_repr));
        if (gtid_slave == NULL) {
            serverLog(LL_WARNING,
                    "[xsync] Partial xsync from %s rejected: invalid gtid.set %s",
                    replicationGetSlaveName(c), gtid_repr);
            goto need_full_resync;
        }

        if (server.gtid_seq == NULL) {
            gtid_xsync = gtidSetNew();
            psync_offset = server.master_repl_offset;
            serverLog(LL_NOTICE, "[xsync] continue point defaults to tail: gtid.seq not exists.");
        } else {
            psync_offset = gtidSeqXsync(server.gtid_seq,gtid_slave,&gtid_xsync);
            sds gtid_xsync_repr = gtidSetDump(gtid_xsync);
            serverLog(LL_NOTICE,
                    "[xsync] continue point: offset=%lld, gtid.set-xsync=%s",
                    psync_offset, gtid_xsync_repr);
            sdsfree(gtid_xsync_repr);
        }

        if (psync_offset < 0) {
            if (server.repl_mode->mode == REPL_MODE_XSYNC) {
                psync_offset = server.master_repl_offset;
                serverLog(LL_NOTICE,
                        "[xsync] continue point adjust to tail: offset=%lld",
                        psync_offset);
            } else {
                psync_offset = MAX(server.repl_mode->from,server.repl_backlog_off);
                serverLog(LL_NOTICE,
                        "[xsync] continue point adjust to from: offset=%lld",
                        psync_offset);
            }
        }
    } else {
        request_mode = REPL_MODE_UNSET;
        serverLog(LL_WARNING,
                    "[gtid] Partial sync from %s rejected: invalid mode %s",
                    replicationGetSlaveName(c), mode);
        goto need_full_resync;
    }

    locate_mode = locateServerReplMode(psync_offset, &switch_mode);
    serverLog(LL_NOTICE, "[gtid] located repl mode: %s, switch: %d",
            replModeName(locate_mode), switch_mode);

    if (request_mode == locate_mode) {
        if (request_mode == REPL_MODE_PSYNC) {
            result = masterTryPartialResynchronization(c);
        } else {
            int fullresync = 0;
            sds gtid_master_repr, gtid_executed_repr, gtid_lost_repr,
                gtid_continue_repr, gtid_xsync_repr, gtid_slave_repr,
                gtid_mlost_repr, gtid_slost_repr;
            sds gtid_updated_lost_repr = NULL;

            gtid_cont = gtidSetDup(server.gtid_executed);
            gtidSetMerge(gtid_cont,server.gtid_lost);

            /* FullResync if gtidSet not related, for example:
             *   empty slave asks for xsync
             *   slave of another shard asks for xsync */
            if (!gtidSetRelated(gtid_cont,gtid_slave)) goto need_full_resync;

            gtid_master_repr = gtidSetDump(gtid_cont);
            gtid_executed_repr = gtidSetDump(server.gtid_executed);
            gtid_lost_repr = gtidSetDump(server.gtid_lost);
            serverLog(LL_NOTICE, "[xsync] gtid.set-master(%s) = gtid.set-executed(%s) + gtid.set-lost(%s)", gtid_master_repr, gtid_executed_repr,gtid_lost_repr);

            gtidSetDiff(gtid_cont,gtid_xsync);

            gtid_continue_repr = gtidSetDump(gtid_cont);
            gtid_xsync_repr = gtidSetDump(gtid_xsync);
            serverLog(LL_NOTICE, "[xsync] gtid.set-continue(%s) = gtid.set-master(%s) - gtid.set-xsync(%s)", gtid_continue_repr,gtid_master_repr,gtid_xsync_repr);

            gtid_mlost = gtidSetDup(gtid_cont);
            gtidSetDiff(gtid_mlost,gtid_slave);

            gtid_slave_repr = gtidSetDump(gtid_slave);
            gtid_mlost_repr = gtidSetDump(gtid_mlost);
            serverLog(LL_NOTICE, "[xsync] gtid.set-mlost(%s) = gtid.set-continue(%s) - gtid.set-slave(%s)", gtid_mlost_repr,gtid_continue_repr,gtid_slave_repr);

            gtid_slost = gtidSetDup(gtid_slave);
            gtidSetDiff(gtid_slost,gtid_cont);

            gtid_slost_repr = gtidSetDump(gtid_slost);
            serverLog(LL_NOTICE, "[xsync] gtid.set-slost(%s) = gtid.set-slave(%s) - gtid.set-continue(%s)", gtid_slost_repr,gtid_slave_repr,gtid_continue_repr);

            gno_t gap = gtidSetCount(gtid_mlost) + gtidSetCount(gtid_slost);
            if (gap > max_gap) {
                serverLog(LL_NOTICE, "[xsync] Partial xsync request from %s rejected: gap=%lld, max_gap=%lld", replicationGetSlaveName(c), gap, max_gap);
                fullresync = 1;
            } else {
                serverLog(LL_NOTICE, "[xsync] Partial xsync request from %s accepted: gap=%lld, max_gap=%lld", replicationGetSlaveName(c), gap, max_gap);

                gtidSetMerge(server.gtid_lost,gtid_mlost);
                gtid_updated_lost_repr = gtidSetDump(server.gtid_lost);

                serverLog(LL_NOTICE, "[xsync] gtid.set-lost(%s) = gtid.set-lost(%s) + gtid.set-mlost(%s)", gtid_updated_lost_repr, gtid_lost_repr, gtid_mlost_repr);

                masterSetupPartialXsynchronization(c,psync_offset,gtid_cont);
            }

            sdsfree(gtid_master_repr), sdsfree(gtid_executed_repr), sdsfree(gtid_lost_repr);
            sdsfree(gtid_continue_repr), sdsfree(gtid_xsync_repr), sdsfree(gtid_slave_repr);
            sdsfree(gtid_mlost_repr), sdsfree(gtid_slost_repr);
            if (gtid_updated_lost_repr) sdsfree(gtid_updated_lost_repr);

            if (fullresync) goto need_full_resync;

            return C_OK;
        }
    } else if (switch_mode) {
        serverLog(LL_NOTICE, "[gtid] repl mode switch from %s to %s",
                replModeName(request_mode),replModeName(locate_mode));

        if (locate_mode == REPL_MODE_XSYNC) {
            sds gtid_master_repr, gtid_executed_repr, gtid_lost_repr,
                gtid_continue_repr, gtid_xsync_repr;

            if (server.gtid_seq == NULL) {
                serverLog(LL_NOTICE,
                        "[xsync] Partial sync request from %s rejected: gtid seq not exist.",
                        replicationGetSlaveName(c));
                goto need_full_resync;
            }

            gtid_cont = gtidSetDup(server.gtid_executed);
            gtidSetMerge(gtid_cont,server.gtid_lost);

            gtid_master_repr = gtidSetDump(gtid_cont);
            gtid_executed_repr = gtidSetDump(server.gtid_executed);
            gtid_lost_repr = gtidSetDump(server.gtid_lost);
            serverLog(LL_NOTICE, "[xsync] gtid.set-master(%s) = gtid.set-executed(%s) + gtid.set-lost(%s)", gtid_master_repr, gtid_executed_repr,gtid_lost_repr);

            gtid_xsync = gtidSeqPsync(server.gtid_seq,psync_offset);
            gtidSetDiff(gtid_cont,gtid_xsync);

            gtid_continue_repr = gtidSetDump(gtid_cont);
            gtid_xsync_repr = gtidSetDump(gtid_xsync);
            serverLog(LL_NOTICE, "[xsync] gtid.set-continue(%s) = gtid.set-master(%s) - gtid.set-xsync(%s)", gtid_continue_repr,gtid_master_repr,gtid_xsync_repr);

            sdsfree(gtid_master_repr), sdsfree(gtid_executed_repr), sdsfree(gtid_lost_repr);
            sdsfree(gtid_continue_repr), sdsfree(gtid_xsync_repr);

            masterSetupPartialXsynchronization(c,psync_offset,gtid_cont);
            return C_OK;
        } else {
            masterSetupPartialResynchronization(c,psync_offset);
            return C_OK;
        }
    } else {
        serverLog(LL_WARNING, "[gtid] Partial sync request from %s rejected: request repl mode(%s) not match located repl mode(%s) at offset(%lld)", replicationGetSlaveName(c),replModeName(request_mode),replModeName(locate_mode),psync_offset);
        goto need_full_resync;
    }

    if (gtid_cont) gtidSetFree(gtid_cont);
    if (gtid_slave) gtidSetFree(gtid_slave);
    if (gtid_gap) gtidSetFree(gtid_gap);
    if (gtid_xsync) gtidSetFree(gtid_xsync);
    return result;

need_full_resync:
    if (gtid_cont) gtidSetFree(gtid_cont);
    if (gtid_slave) gtidSetFree(gtid_slave);
    if (gtid_gap) gtidSetFree(gtid_gap);
    if (gtid_xsync) gtidSetFree(gtid_xsync);
    return C_ERR;
}

/* NOTE: Must keep following macros in-sync */
#define PSYNC_WRITE_ERROR 0
#define PSYNC_WAIT_REPLY 1
#define PSYNC_CONTINUE 2
#define PSYNC_FULLRESYNC 3
#define PSYNC_NOT_SUPPORTED 4
#define PSYNC_TRY_LATER 5

int ctrip_slaveTryPartialResynchronizationWrite(connection *conn) {
    gtidSet *gtid_slave = NULL;
    char max_gap[32];
    int result = PSYNC_WAIT_REPLY;
    sds gtid_slave_repr, gtid_executed_repr, gtid_lost_repr;

    serverLog(LL_NOTICE, "[gtid] Trying parital sync in (%s) mode.",
            replModeName(server.repl_mode->mode));

    if (server.repl_mode->mode != REPL_MODE_XSYNC) return -1;

    /* Explicitly clear master_replid/master_initial_offset to avoid any
     * further usage. Note that offset was intentionally set to 0 to avoid
     * flagging master client as CLIENT_PRE_PSYNC. */
    server.master_initial_offset = 0;
    memset(server.master_replid,0,CONFIG_RUN_ID_SIZE+1);

    snprintf(max_gap,sizeof(max_gap),"%lld",server.gtid_xsync_max_gap);

    gtid_slave = gtidSetDup(server.gtid_executed);
    gtidSetMerge(gtid_slave,server.gtid_lost);

    gtid_executed_repr = gtidSetDump(server.gtid_executed);
    gtid_lost_repr = gtidSetDump(server.gtid_lost);
    gtid_slave_repr = gtidSetDump(gtid_slave);
    serverLog(LL_NOTICE, "[xsync] gtid.set-slave(%s) = gtid.set-executed(%s) + gtid.set-lost(%s)",gtid_slave_repr,gtid_executed_repr,gtid_lost_repr);
    serverLog(LL_NOTICE, "[xsync] Trying partial xsync with gtid.set=%s, maxgap=%s",gtid_slave_repr,max_gap);

    sds reply = sendCommand(conn,"XSYNC","*",gtid_slave_repr,
            "MAXGAP",max_gap,NULL);
    if (reply != NULL) {
        serverLog(LL_WARNING,"[xsync] Unable to send XSYNC: %s", reply);
        sdsfree(reply);
        connSetReadHandler(conn, NULL);
        result = PSYNC_WRITE_ERROR;
    }

    gtidSetFree(gtid_slave);
    sdsfree(gtid_slave_repr), sdsfree(gtid_executed_repr);
    sdsfree(gtid_lost_repr);

    return result;
}

static void ctrip_replicationMasterLinkUp() {
    server.repl_state = REPL_STATE_CONNECTED;
    server.repl_down_since = 0;
    /* Fire the master link modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
            REDISMODULE_SUBEVENT_MASTER_LINK_UP,
            NULL);
}

int ctrip_slaveTryPartialResynchronizationRead(connection *conn, sds reply) {
    if (server.repl_mode->mode != REPL_MODE_XSYNC) {
        if (!strncmp(reply,"+XFULLRESYNC",12)) {
            /* PSYNC => XFULLRESYNC */
            serverLog(LL_NOTICE,
                    "[gtid] repl mode switch: psync => xsync (xfullresync)");
            replicationDiscardCachedMaster();
            shiftServerReplMode(REPL_MODE_XSYNC);
            sdsfree(reply);
            return PSYNC_FULLRESYNC;
        }

        if (!strncmp(reply,"+XCONTINUE",10)) {
            /* PSYNC => XCONTINUE */
            serverLog(LL_NOTICE,
                    "[gtid] repl mode switch: psync => xsync (xcontinue)");

            sds *tokens;
            size_t token_off = 10;
            int i, ntoken, result = PSYNC_CONTINUE;

            while (token_off < sdslen(reply) && isspace(reply[token_off]))
                token_off++;
            tokens = sdssplitlen(reply+token_off,
                    sdslen(reply)-token_off, " ",1,&ntoken);

            for (i = 0; i+1 < ntoken; i += 2) {
                if (!strncasecmp(tokens[i], "gtid.set", sdslen(tokens[i]))) {
                    gtidSet *gtid_cont = NULL, *gtid_slave = NULL;

                    gtid_cont = gtidSetDecode(tokens[i+1],sdslen(tokens[i+1]));
                    if (gtid_cont == NULL) {
                        serverLog(LL_WARNING, "[xsync] Parsed invalid gtid.set-cont(%s)", tokens[i+1]);
                        result = PSYNC_TRY_LATER;
                        break;
                    }

                    gtid_slave = gtidSetDup(server.gtid_executed);
                    gtidSetMerge(gtid_slave,server.gtid_lost);

                    if (!gtidSetEqual(gtid_slave,gtid_cont)) {
                        sds gtid_slave_repr = gtidSetDump(gtid_slave);
                        serverLog(LL_WARNING,"[xsync] gtid.set-slave(%s) != gtid.set-cont(%s)",gtid_slave_repr, tokens[i+1]);
                        sdsfree(gtid_slave_repr);
                        result = PSYNC_TRY_LATER;
                    }
                    gtidSetFree(gtid_cont);
                    gtidSetFree(gtid_slave);
                } else {
                    serverLog(LL_NOTICE,
                            "[xsync] Ignored xcontinue option: %s", tokens[i]);
                }
            }

            if (result == PSYNC_CONTINUE) {
                replicationDiscardCachedMaster();
                disconnectSlaves();
                shiftServerReplMode(REPL_MODE_XSYNC);
                if (server.repl_backlog == NULL)
                    ctrip_createReplicationBacklog();
            }

            sdsfreesplitres(tokens,ntoken);
            sdsfree(reply);
            return result;
        }
        /* psync => fullresync, psync => continue, unknown response handled
         * by origin psync. */
        return -1;
    } else {
        if (!strncmp(reply,"+FULLRESYNC",11)) {
            /* XSYNC => FULLRESYNC */
            serverLog(LL_NOTICE,
                    "[gtid] repl mode switch: xsync => psync (fullresync)");

            shiftServerReplMode(REPL_MODE_PSYNC);
            /* handled by origin psync */
            return -1;
        }

        if (!strncmp(reply,"+CONTINUE",9)) {
            /* XSYNC => CONTINUE */
            serverLog(LL_NOTICE,
                    "[gtid] repl mode switch: xsync => psync (continue)");
            serverAssert(!server.master && !server.cached_master);

            char *start, *end, new[CONFIG_RUN_ID_SIZE+1];

            /* parse replid */
            start = reply+9;
            while(start[0] == ' ' || start[0] == '\t') start++;
            end = start;
            while(end[0] != ' ' && end[0] != '\t' &&
                    end[0] != '\r' && end[0] != '\n' && end[0] != '\0') end++;

            if (end-start == CONFIG_RUN_ID_SIZE) {
                memcpy(new,start,CONFIG_RUN_ID_SIZE);
                new[CONFIG_RUN_ID_SIZE] = '\0';
            } else {
                serverLog(LL_WARNING,"[xsync] got invalid replid:%s", reply);
                sdsfree(reply);
                return PSYNC_TRY_LATER;
            }

            /* parse offset */
            long long offset;
            start = end;
            while(start[0] == ' ' || start[0] == '\t') start++;
            end = start;
            while(end[0] != '\r' && end[0] != '\n' && end[0] != '\0') end++;
            if (string2ll(start,end-start,&offset) == 0) {
                serverLog(LL_WARNING,"[xsync] got invalid offset:%s",reply);
                sdsfree(reply);
                return PSYNC_TRY_LATER;
            }

            /* Create master client with args specified in CONTINUE reply. */
            memcpy(server.master_replid,new,sizeof(server.master_replid));
            server.master_initial_offset = offset;
            replicationCreateMasterClient(conn,-1);

            disconnectSlaves();
            shiftServerReplMode(REPL_MODE_PSYNC);
            sdsfree(reply);
            return PSYNC_CONTINUE;
        }

        if (!strncmp(reply,"+XFULLRESYNC",12)) {
            /* XSYNC => XFULLRESYNC */
            serverLog(LL_NOTICE,"[xsync] XFullResync from master: %s.", reply);
            serverAssert(!server.cached_master && !server.master);
            sdsfree(reply);
            return PSYNC_FULLRESYNC;
        }

        if (!strncmp(reply,"+XCONTINUE",10)) {
            /* XSYNC => XCONTINUE */
            sds *tokens;
            size_t token_off = 10;
            int i, ntoken, result = PSYNC_CONTINUE;

            serverLog(LL_NOTICE, "[xsync] Successful partial xsync with master: %s", reply);

            while (token_off < sdslen(reply) && isspace(reply[token_off]))
                token_off++;
            tokens = sdssplitlen(reply+token_off,
                    sdslen(reply)-token_off," ",1,&ntoken);

            for (i = 0; i+1 < ntoken; i += 2) {
                if (!strncasecmp(tokens[i], "gtid.set", sdslen(tokens[i]))) {
                    gtidSet *gtid_cont = NULL, *gtid_slost = NULL,
                            *gtid_slave = NULL;
                    sds gtid_cont_repr, gtid_lost_repr,
                        gtid_slave_repr, gtid_executed_repr,
                        gtid_slost_repr, gtid_updated_lost_repr;

                    gtid_cont = gtidSetDecode(tokens[i+1],sdslen(tokens[i+1]));
                    if (gtid_cont == NULL) {
                        serverLog(LL_WARNING, "[xsync] invalid gtid.set-cont(%s), default gtid.set-cont to ()", tokens[i+1]);
                        result = PSYNC_TRY_LATER;
                        break;
                    }

                    gtid_slave = gtidSetDup(server.gtid_executed);
                    gtidSetMerge(gtid_slave,server.gtid_lost);

                    gtid_executed_repr = gtidSetDump(server.gtid_executed);
                    gtid_lost_repr = gtidSetDump(server.gtid_lost);
                    gtid_slave_repr = gtidSetDump(gtid_slave);
                    serverLog(LL_NOTICE, "[xsync] gtid.set-slave(%s) = gtid.set-executed(%s) + gtid.set-lost(%s)", gtid_slave_repr,gtid_executed_repr,gtid_lost_repr);

                    gtid_slost = gtidSetDup(gtid_cont);
                    gtidSetDiff(gtid_slost,gtid_slave);

                    gtid_cont_repr = gtidSetDump(gtid_cont);
                    gtid_slost_repr = gtidSetDump(gtid_slost);
                    serverLog(LL_NOTICE, "[xsync] gtid.set-slost(%s) = gtid.set-continue(%s) - gtid.set-slave(%s)", gtid_slost_repr,gtid_cont_repr,gtid_slave_repr);

                    gtidSetMerge(server.gtid_lost, gtid_slost);
                    gtid_updated_lost_repr = gtidSetDump(server.gtid_lost);
                    serverLog(LL_NOTICE, "[xsync] gtid.set-lost(%s) = gtid.set-lost(%s) + gtid.set-slost(%s)", gtid_updated_lost_repr,gtid_lost_repr,gtid_slost_repr);

                    if (gtidSetCount(gtid_slost)) {
                        serverLog(LL_NOTICE, "[xsync] Disconnect slaves to notify my gtid.lost updated.");
                        disconnectSlaves();
                    }

                    sdsfree(gtid_cont_repr), sdsfree(gtid_lost_repr);
                    sdsfree(gtid_slave_repr), sdsfree(gtid_executed_repr);
                    sdsfree(gtid_slost_repr), sdsfree(gtid_updated_lost_repr);
                } else {
                    serverLog(LL_NOTICE,
                            "[xsync] Ignored xcontinue option: %s", tokens[i]);
                }
            }

            if (result == PSYNC_CONTINUE) {
                replicationCreateMasterClient(conn,-1);
                ctrip_replicationMasterLinkUp();
                if (server.repl_backlog == NULL)
                    ctrip_createReplicationBacklog();
            }

            sdsfree(reply);
            sdsfreesplitres(tokens,ntoken);
            return result;
        }

        /* unknown response handled by origin psync. */
        return -1;
    }
}

sds genGtidInfoString(sds info) {
    gtidStat executed_stat, lost_stat;
    gtidSetGetStat(server.gtid_executed, &executed_stat);
    gtidSetGetStat(server.gtid_lost, &lost_stat);
    sds gtid_executed_repr = gtidSetDump(server.gtid_executed);
    sds gtid_lost_repr = gtidSetDump(server.gtid_lost);

    info  = sdscatprintf(info,
            "gtid_uuid:%s\r\n"
            "gtid_executed:%s\r\n"
            "gtid_executed_gno_count:%lld\r\n"
            "gtid_executed_used_memory:%lu\r\n"
            "gtid_lost:%s\r\n"
            "gtid_lost_gno_count:%lld\r\n"
            "gtid_lost_used_memory:%lu\r\n"
            "gtid_repl_mode:%s\r\n"
            "gtid_repl_from:%lld\r\n"
            "gtid_prev_repl_mode:%s\r\n"
            "gtid_prev_repl_from:%lld\r\n"
            "gtid_executed_cmd_count:%lu\r\n"
            "gtid_ignored_cmd_count:%lu\r\n",
            server.uuid,
            gtid_executed_repr,
            executed_stat.gno_count,
            executed_stat.used_memory,
            gtid_lost_repr,
            lost_stat.gno_count,
            lost_stat.used_memory,
            replModeName(server.repl_mode->mode),
            server.repl_mode->from,
            replModeName(server.prev_repl_mode->mode),
            server.prev_repl_mode->from,
            server.gtid_executed_cmd_count,
            server.gtid_ignored_cmd_count);

    sdsfree(gtid_executed_repr);
    sdsfree(gtid_lost_repr);
    return info;
}

sds catGtidStatString(sds info, gtidStat *stat) {
    return sdscatprintf(info,
            "uuid_count:%ld,used_memory:%ld,gap_count:%ld,gno_count:%lld",
            stat->uuid_count, stat->used_memory, stat->gap_count,
            stat->gno_count);
}

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
            "SEQ XSYNC <gtid.set>",
            "    Locate xsync continue position",
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
        } else if (!strcasecmp(c->argv[2]->ptr,"xsync") && c->argc >= 4) {
            if (server.gtid_seq) {
                long long blmaxlen = 128;
                char *blbuf;
                size_t bllen = 0;
                sds gtidrepr = c->argv[3]->ptr;
                gtidSet *gtid_req = NULL,*gtid_cont = NULL;
                gtid_req = gtidSetDecode(gtidrepr,sdslen(gtidrepr));
                if (c->argc > 4) getLongLongFromObject(c->argv[4],&blmaxlen);
                blbuf = zcalloc(blmaxlen);
                long long offset = gtidSeqXsync(server.gtid_seq,gtid_req,&gtid_cont);
                size_t maxlen = gtidSetEstimatedEncodeBufferSize(gtid_cont);
                char *buf = zmalloc(maxlen);
                size_t len = gtidSetEncode(buf,maxlen,gtid_cont);
                addReplyArrayLen(c,3);
                addReplyBulkLongLong(c,offset);
                addReplyBulkCBuffer(c,buf,len);
                if (offset >= 0) {
                    bllen = copyReplicationBacklogLimited(blbuf,blmaxlen-1,offset);
                    addReplyBulkCBuffer(c,blbuf,bllen);
                } else {
                    addReplyNull(c);
                }
                zfree(buf);
                gtidSetFree(gtid_cont);
                gtidSetFree(gtid_req);
            } else {
                addReplyError(c, "gtid seq not exists");
            }
        } else if (!strcasecmp(c->argv[2]->ptr,"gtid.set") && c->argc == 3) {
            if (server.gtid_seq) {
                gtidSegment *seg;
                gtidSet *gtid_seq = gtidSetNew();
                for (seg = server.gtid_seq->firstseg; seg != NULL;
                        seg = seg->next) {
                    gtidSetAdd(gtid_seq,seg->uuid,seg->uuid_len,
                            seg->base_gno+seg->tgno,seg->base_gno+seg->ngno-1);
                }
                size_t maxlen = gtidSetEstimatedEncodeBufferSize(gtid_seq);
                char *buf = zmalloc(maxlen);
                size_t len = gtidSetEncode(buf,maxlen,gtid_seq);
                addReplyBulkCBuffer(c,buf,len);
                zfree(buf);
                gtidSetFree(gtid_seq);
            } else {
                addReplyError(c, "gtid seq not exists");
            }
        } else {
            addReplyError(c,"Syntax error");
        }
    } else {
        addReplySubcommandSyntaxError(c);
    }

}

