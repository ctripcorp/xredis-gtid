#include "server.h"
#include <gtid.h>

#define GTID_COMMAN_ARGC 3

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
#define GTID_UUID_PURGE_INTERVAL 16
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

        /* Try to purge after 16 gno added to uuid_set */
        if (uuidSetCount(uuid_set)%GTID_UUID_PURGE_INTERVAL == 0) {
            uuidSetPurged purged;
            uuidSetPurge(uuid_set, server.gtid_uuid_gap_max_memory, &purged);
            if (purged.gno_count > 0) {
                server.gtid_last_purge_time = server.mstime;
                server.gtid_purged_gap_count += purged.node_count;
                server.gtid_purged_gno_count += purged.gno_count;
                serverLog(LL_NOTICE,"[gtid] purged %.*s: gap=%ld, gno=%lld, start=%lld, end=%lld",
                        (int)uuid_len, uuid, purged.node_count,
                        purged.gno_count, purged.start, purged.end);
            }
        }
    } else {
        result = gtidSetAdd(server.gtid_executed, uuid, uuid_len, gno);
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
    int argc, dbid, uuid_len = server.current_uuid->uuid_len;
    robj **argv;
    char *buf, *uuid = server.current_uuid->uuid;
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
        gno = uuidSetNext(server.current_uuid, 1);

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

int rdbSaveGtidInfoAuxFields(rio* rdb) {
    size_t maxlen = gtidSetEstimatedEncodeBufferSize(server.gtid_executed);
    char *gtid_repr = zmalloc(maxlen);
    size_t gtid_repr_len = gtidSetEncode(gtid_repr, maxlen, server.gtid_executed);
    if (rdbSaveAuxField(rdb, "gtid", 4, gtid_repr, gtid_repr_len) == -1) {
        zfree(gtid_repr);
        return -1;
    }
    zfree(gtid_repr);
    return 1;
}

int LoadGtidInfoAuxFields(robj* key, robj* val) {
    if (!strcasecmp(key->ptr, "gtid")) {
        if(server.gtid_executed != NULL) {
            gtidSetFree(server.gtid_executed);
            server.gtid_executed = NULL;
        }
        server.gtid_executed = gtidSetDecode(val->ptr, sdslen(val->ptr));
        server.current_uuid = gtidSetFind(server.gtid_executed, server.runid, strlen(server.runid));
        if (server.current_uuid == NULL) {
            gtidSetAdd(server.gtid_executed, server.runid, strlen(server.runid), 0);
            server.current_uuid = gtidSetFind(server.gtid_executed, server.runid, strlen(server.runid));
        }
        return 1;
    }
    return 0;
}

void shiftServerReplMode(int mode) {
    serverAssert(mode != REPL_MODE_UNSET);
    if (server.repl_mode->mode == mode) return;
    server.prev_repl_mode->from = server.repl_mode->from;
    server.prev_repl_mode->mode = server.repl_mode->mode;
    server.repl_mode->from = server.master_repl_offset;
    server.repl_mode->mode = mode;
}

static inline void updateServerReplMode() {
    /* Note that we must not update from in replMode: from indicates the
     * offset that repl mode can switch. */
    serverAssert(server.repl_mode->mode != REPL_MODE_UNSET);
    if (server.prev_repl_mode->mode != REPL_MODE_UNSET) {
        if (server.repl_backlog_off >= server.repl_mode->from) {
            server.prev_repl_mode->mode = REPL_MODE_UNSET;
        }
    }
}

int locateServerReplMode(long long offset, int *switch_mode) {
    serverAssert(server.repl_mode->mode != REPL_MODE_UNSET);
    if (offset >= server.repl_mode->from) {
        *switch_mode = offset == server.repl_mode->from;
        return server.repl_mode->mode;
    }
    if (server.prev_repl_mode->mode == REPL_MODE_UNSET) {
        *switch_mode = 0;
        return REPL_MODE_UNSET;
    }
    if (offset >= server.prev_repl_mode->from) {
        *switch_mode = offset == server.repl_mode->from;
        return server.prev_repl_mode->mode;
    }
    *switch_mode = 0;
    return REPL_MODE_UNSET;
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
    int touch_index = uuid != NULL && gno >= GNO_INITIAL && server.gtid_seq
        && server.masterhost == NULL;
    if (touch_index) gtidSeqAppend(server.gtid_seq,uuid,uuid_len,gno,offset);
    replicationFeedSlaves(slaves,dictid,argv,argc);
    if (touch_index) gtidSeqTrim(server.gtid_seq,server.repl_backlog_off);
    updateServerReplMode();
}

void ctrip_replicationFeedSlavesFromMasterStream(list *slaves, char *buf,
        size_t buflen, const char *uuid, size_t uuid_len, gno_t gno, long long offset) {
    int touch_index = uuid != NULL && gno >= GNO_INITIAL && server.gtid_seq;
    if (touch_index) gtidSeqAppend(server.gtid_seq,uuid,uuid_len,gno,offset);
    replicationFeedSlavesFromMasterStream(slaves,buf,buflen);
    if (touch_index) gtidSeqTrim(server.gtid_seq,server.repl_backlog_off);
    updateServerReplMode();
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

int masterSetupPartialSynchronization(client *c, long long psync_offset,
        char *buf, int buflen) {
    int disconect;
    long long psync_len;

    /* see masterTryPartialResynchronization for more details. */
    /* If we reached this point, we are able to perform a partial xsync:
     * 1) Set client state to make it a slave.
     * 2) Inform the client we can continue with +XCONTINUE
     * 3) Send the backlog data (from the offset to the end) to the slave. */
    c->flags |= CLIENT_SLAVE;
    c->replstate = SLAVE_STATE_ONLINE;
    c->repl_ack_time = server.unixtime;
    c->repl_put_online_on_ack = 0;
    listAddNodeTail(server.slaves,c);
    /* We can't use the connection buffers since they are used to accumulate
     * new commands at this stage. But we are sure the socket send buffer is
     * empty so this write will never fail actually. */

    if (connWrite(c->conn,buf,buflen) != buflen) {
        freeClientAsync(c);
        return C_ERR;
    }

    disconect = ctrip_addReplyReplicationBacklog(c,psync_offset,&psync_len);
    serverLog(LL_NOTICE,
        "Sending %lld bytes of backlog starting from offset %lld.",
        psync_len, psync_offset);

    if (disconect) {
        //TODO confirm that backlog will be sent and then connection closed
        serverLog(LL_NOTICE, "Closing slave %s to switch repl mode.",
                replicationGetSlaveName(c));
        freeClientAsync(c);
        //TODO return what?
        return C_ERR;
    }

    /* Note that we don't need to set the selected DB at server.slaveseldb
     * to -1 to force the master to emit SELECT, since the slave already
     * has this state from the previous connection with the master. */

    refreshGoodSlavesCount();

    /* Fire the replica change modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICA_CHANGE,
                          REDISMODULE_SUBEVENT_REPLICA_CHANGE_ONLINE,
                          NULL);

    return C_OK; /* The caller can return, no full resync needed. */
}

int masterSetupPartialXsynchronization(client *c, long long psync_offset,
        gtidSet *gtid_cont) {
    char *gtidrepr, *buf;
    int gtidlen, buflen, ret;

    gtidlen = gtidSetEstimatedEncodeBufferSize(gtid_cont);
    gtidrepr = zcalloc(gtidlen);
    gtidSetEncode(gtidrepr, gtidlen, server.gtid_executed);
    buflen = gtidlen+256;
    buf = zcalloc(buflen);
    serverAssert(psync_offset >= server.repl_backlog_off &&
            psync_offset <= server.master_repl_offset);
    buflen = snprintf(buf,buflen,
            "+XCONTINUE GTID.SET %.*s MASTER.SID %.*s\r\n",
             gtidlen,gtidrepr,(int)server.current_uuid->uuid_len,
             server.current_uuid->uuid);
    ret =  masterSetupPartialSynchronization(c,psync_offset,buf,buflen);
    zfree(buf);
    zfree(gtidrepr);
    return ret;
}

int masterSetupPartialResynchronization(client *c, long long psync_offset) {
    char buf[128];
    int buflen;
    serverAssert(psync_offset >= server.repl_backlog_off &&
            psync_offset <= server.master_repl_offset);
    buflen = snprintf(buf,sizeof(buf),"+CONTINUE %s\r\n",
             server.current_uuid->uuid);
    return masterSetupPartialSynchronization(c,psync_offset,buf,buflen);
}

int ctrip_masterTryPartialResynchronization(client *c) {
    long long psync_offset, maxgap = 0;
    gtidSet *gtid_slave = NULL, *gtid_cont = NULL, *gtid_xsync = NULL,
            *gtid_gap = NULL, *gtid_mlost = NULL, *gtid_slost = NULL;
    const char *mode = c->argv[0]->ptr;
    int request_mode, locate_mode, switch_mode, result;

    /* get paritial sync request offset:
     *   psync: request offset specified in command
     *   xsync: offset of continue point
     */
    if (!strcasecmp(mode,"psync")) {
        request_mode = REPL_MODE_PSYNC;
        if (getLongLongFromObjectOrReply(c,c->argv[2],&psync_offset,NULL)
                != C_OK) goto need_full_resync;
    } else if (!strcasecmp(mode,"xsync")) {
        sds gtid_repr = c->argv[2]->ptr;
        request_mode = REPL_MODE_XSYNC;
        gtid_slave = gtidSetDecode(gtid_repr,sdslen(gtid_repr));
        if (gtid_slave == NULL) goto need_full_resync;
        for (int i = 3; i+1 < c->argc; i += 2) {
            if (!strcasecmp(c->argv[i]->ptr,"tolerate")) {
                if (getLongLongFromObjectOrReply(c,c->argv[i+1],&maxgap,NULL)
                        != C_OK) {
                    serverLog(LL_WARNING,
                            "tolerate option with invalid maxgap: %s",
                            (sds)c->argv[i+1]->ptr);
                }
            } else {
                serverLog(LL_WARNING, "unknown xsync option ignored: %s",
                        (sds)c->argv[i]->ptr);
            }
        }
        if (server.gtid_seq == NULL) {
            serverLog(LL_NOTICE,"Partial xsync request from %s rejected: gtid seq not exist.", replicationGetSlaveName(c));
            goto need_full_resync;
        }
        psync_offset = gtidSeqXsync(server.gtid_seq,gtid_slave,&gtid_xsync);
        //TODO 不一定就是最后一个，可能是switch mode点？应该调整到上一个xsync mode结束的点
        if (psync_offset < 0) psync_offset = server.master_repl_offset;
    } else {
        request_mode = REPL_MODE_UNSET;
        goto need_full_resync;
    }

    /* check if request repl mode valid(switch repl mode if needed). */
    locate_mode = locateServerReplMode(psync_offset, &switch_mode);
    if (locate_mode == REPL_MODE_UNSET) goto need_full_resync;

    if (request_mode == locate_mode) {
        if (request_mode == REPL_MODE_PSYNC) {
            result = masterTryPartialResynchronization(c);
        } else {
            gtid_cont = gtidSetDup(server.gtid_executed);
            gtidSetMerge(gtid_cont,gtidSetDup(server.gtid_lost));
            gtidSetDiff(gtid_cont,gtid_xsync);

            gtid_mlost = gtidSetDup(gtid_cont);
            gtidSetDiff(gtid_mlost,gtid_slave);
            gtid_slost = gtidSetDup(gtid_slave);
            gtidSetDiff(gtid_slost,gtid_cont);

            // serverLog(LL_NOTICE, "Master parital xsync with gtid.set: executed=%s,
            // lost=%s, xsync=%s, cont=%s, mlost=%s, slost=%s");

            gno_t gap = gtidSetCount(gtid_mlost) + gtidSetCount(gtid_slost);
            if (gap > maxgap) {
                serverLog(LL_NOTICE, "Partial xsync request from %s rejected: gap=%lld, maxgap=%lld", replicationGetSlaveName(c), gap, maxgap);
                goto need_full_resync;
            } else {
                serverLog(LL_NOTICE, "Partial xsync request from %s accepted: gap=%lld, maxgap=%lld", replicationGetSlaveName(c), gap, maxgap);
                result = masterSetupPartialXsynchronization(c,psync_offset,gtid_cont);
            }
        }
    } else if (switch_mode) {
        if (request_mode == REPL_MODE_PSYNC) {
            if (server.gtid_seq == NULL) {
                serverLog(LL_NOTICE,"Partial resync request from %s rejected: gtid seq not exist.", replicationGetSlaveName(c));
                goto need_full_resync;
            }
            gtid_cont = gtidSetDup(server.gtid_executed);
            gtidSetMerge(gtid_cont,gtidSetDup(server.gtid_lost));
            gtid_xsync = gtidSeqPsync(server.gtid_seq,psync_offset);
            gtidSetDiff(gtid_cont,gtid_xsync);

            result = masterSetupPartialXsynchronization(c,psync_offset,gtid_cont);
        } else {
            result = masterSetupPartialResynchronization(c,psync_offset);
        }
    } else {
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

/* NOTE: Must keep following mocros in-sync */
#define PSYNC_WRITE_ERROR 0
#define PSYNC_WAIT_REPLY 1
#define PSYNC_CONTINUE 2
#define PSYNC_FULLRESYNC 3
#define PSYNC_NOT_SUPPORTED 4
#define PSYNC_TRY_LATER 5

char *sendCommand(connection *conn, ...);

int ctrip_slaveTryPartialResynchronizationWrite(connection *conn) {
    gtidSet *gtid_slave = NULL;
    size_t gtidlen;
    char *gtidrepr = NULL;
    int result = PSYNC_WAIT_REPLY;

    if (server.repl_mode->mode != REPL_MODE_XSYNC) return -1;

    gtid_slave = gtidSetDup(server.gtid_executed);
    gtidSetMerge(gtid_slave,server.gtid_lost);//TODO merge no MOVE
    gtidlen = gtidSetEstimatedEncodeBufferSize(gtid_slave);
    gtidrepr = zcalloc(gtidlen);

    gtidSetEncode(gtidrepr, gtidlen, server.gtid_executed);
    sds reply = sendCommand(conn,"XSYNC","*",gtidrepr,NULL);//TODO deal with failover?

    if (reply != NULL) {
        serverLog(LL_WARNING,"Unable to send XSYNC to master: %s", reply);
        sdsfree(reply);
        connSetReadHandler(conn, NULL);
        result = PSYNC_WRITE_ERROR;
    }

    gtidSetFree(gtid_slave);
    zfree(gtidrepr);

    return result;
}

int ctrip_slaveTryPartialResynchronizationRead(connection *conn, sds reply) {
    if (server.repl_mode->mode != REPL_MODE_XSYNC) {
        if (!strncmp(reply,"+XFULLRESYNC",12)) {
            /* psync => xfullresync */
        }
        if (!strncmp(reply,"+XCONTINUE",10)) {
            /* psync => xcontinue */
        }
        /* psync => fullresync, psync => continue, unknown response handled
         * by origin psync. */
        return -1;
    } else {
        if (!strncmp(reply,"+FULLRESYNC",11)) {
            /* xsync => fullresync */
            serverAssert(0);
            sdsfree(reply);
            return -1;
        }

        if (!strncmp(reply,"+CONTINUE",9)) {
            /* xsync => continue */
            serverAssert(0);
            sdsfree(reply);
            return -1;
        }

        if (!strncmp(reply,"+XFULLRESYNC",12)) {
            /* xsync => xfullresync */
            serverLog(LL_NOTICE,"XFull resync from master: %s.", reply);
            sdsfree(reply);
            return PSYNC_FULLRESYNC;
        }

        if (!strncmp(reply,"+XCONTINUE",10)) {
            /* xsync => xcontinue */
            sds *tokens;
            int i, ntoken;
            gtidSet *gtid_cont = NULL, *gtid_lost = NULL;

            tokens = sdssplitlen(reply+10, sdslen(reply)-10, " ", 1, &ntoken);

            for (i = 0; i+1 < ntoken; i += 2) {
                if (strncasecmp(tokens[i], "gtid.set", sdslen(tokens[i]))) {
                    gtid_cont = gtidSetDecode(tokens[i+1],sdslen(tokens[i+1]));
                    gtid_lost = gtidSetDup(gtid_cont);
                    gtidSetDiff(gtid_lost, server.gtid_executed);
                    gtidSetDiff(gtid_lost, server.gtid_lost);
                    // serverLog(LL_NOTICE, "xcontinue with gtid info:"
                    // "server.gtid_executed=%s, server.gtid_lost=%s, gtid.set=%s => gtid.lost=%s",);
                    gtidSetMerge(server.gtid_lost, gtid_lost);
                    // serverLog(LL_NOTICE, "server.gtid_lost updated to %s," );
                } else if (strncasecmp(tokens[i], "master.sid",
                            sdslen(tokens[i]))) {
                    /* ignored */
                } else {
                    serverLog(LL_WARNING,
                            "ignored unknown xcontinue option: %s", tokens[i]);
                }
            }

            sdsfree(reply);
            replicationCreateMasterClient(conn,-1);
            if (server.repl_backlog == NULL) ctrip_createReplicationBacklog();
            serverLog(LL_NOTICE, "Successful xsync with master: %s.", reply);
            return PSYNC_CONTINUE;
        }
        /* unknown response handled by origin psync. */
        return -1;
    }
}

sds genGtidStatString(sds info) {
    gtidStat stat;
    gtidSetGetStat(server.gtid_executed, &stat);
    info = sdscatprintf(info,
            "gtid_used_memory:%lu\r\n"
            "gtid_uuid_count:%lu\r\n"
            "gtid_gap_count:%lu\r\n"
            "gtid_gno_count:%lld\r\n"
            "gtid_purged_gap_count:%lu\r\n"
            "gtid_purged_gno_count:%lld\r\n"
            "gtid_last_purge_time:%ld\r\n"
            "gtid_executed_cmd_count:%lu\r\n"
            "gtid_ignored_cmd_count:%lu\r\n",
            stat.used_memory,
            stat.uuid_count,
            stat.gap_count,
            stat.gno_count,
            server.gtid_purged_gap_count,
            server.gtid_purged_gno_count,
            server.gtid_last_purge_time,
            server.gtid_executed_cmd_count,
            server.gtid_ignored_cmd_count);

    return info;
}

sds catGtidStatString(sds info, gtidStat *stat) {
    return sdscatprintf(info,
            "uuid_count:%ld,used_memory:%ld,gap_count:%ld,gno_count:%lld",
            stat->uuid_count, stat->used_memory, stat->gap_count,
            stat->gno_count);
}

/* gtid manage command */
void gtidxCommand(client *c) {
    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        const char *help[] = {
            "LIST [<uuid>]",
            "    List gtid or uuid(if specified) gaps.",
            "STAT [<uuid>]",
            "    Show gtid or uuid(if specified) stat.",
            "REMOVE <uuid>",
            "    Remove uuid.",
            "SEQ",
            "    List gtid.seq.",
            "SEQ GTID.SET",
            "    Get gtid.set of gtid seq index.",
            "SEQ XSYNC <gtid.set>",
            "    Locate xsync continue position",
            NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(c->argv[1]->ptr,"list") && (c->argc == 2 || c->argc == 3)) {
        char *buf;
        size_t maxlen, len;
        if (c->argc == 2) {
            maxlen = gtidSetEstimatedEncodeBufferSize(server.gtid_executed);
            buf = zcalloc(maxlen);
            len = gtidSetEncode(buf, maxlen, server.gtid_executed);
            addReplyBulkCBuffer(c, buf, len);
            zfree(buf);
        } else {
            sds uuid = c->argv[2]->ptr;
            uuidSet *uuid_set = gtidSetFind(server.gtid_executed, uuid, sdslen(uuid));
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
    } else if (!strcasecmp(c->argv[1]->ptr,"stat") && (c->argc == 2 || c->argc == 3)) {
        gtidStat stat;
        sds stat_str;
        if (c->argc == 2) {
            gtidSetGetStat(server.gtid_executed, &stat);
            stat_str = sdscatprintf(sdsempty(), "all:");
            stat_str = catGtidStatString(stat_str, &stat);
            addReplyBulkSds(c, stat_str);
        } else {
            sds uuid = c->argv[2]->ptr, stat_str;
            uuidSet *uuid_set = gtidSetFind(server.gtid_executed, uuid, sdslen(uuid));
            if (uuid_set == NULL) {
                addReplyErrorFormat(c, "uuid not found:%s", uuid);
            } else {
                uuidSetGetStat(uuid_set, &stat);
                stat_str = sdscatprintf(sdsempty(), "%s:", uuid);
                stat_str = catGtidStatString(stat_str, &stat);
                addReplyBulkSds(c, stat_str);
            }
        }

    } else if (!strcasecmp(c->argv[1]->ptr,"remove") && c->argc == 3) {
        int removed = 0;
        sds uuid = c->argv[2]->ptr;
        if (server.current_uuid->uuid_len == sdslen(uuid) &&
                memcmp(server.current_uuid->uuid, uuid, sdslen(uuid)) == 0) {
            addReplyLongLong(c, 0);
        } else {
            removed = gtidSetRemove(server.gtid_executed, uuid, sdslen(uuid));
            addReplyLongLong(c, removed);
        }
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
                    gtidSetAddRange(gtid_seq,seg->uuid,seg->uuid_len,
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
