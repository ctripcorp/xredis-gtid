#include "server.h"
#include <gtid.h>
#include <ctype.h>

#define GTID_COMMAN_ARGC 3

#define GTID_XSYNC_UUID_INTERESTED_DEFAULT      "*"
#define GTID_XSYNC_UUID_INTERESTED_FULLRESYNC   "?"

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

gtidSet *serverGtidSetGet(char *log_prefix) {
    gtidSet *gtid_master = gtidSetDup(server.gtid_executed);
    gtidSetMerge(gtid_master,server.gtid_lost);
    if (log_prefix != NULL) {
        sds gtid_master_repr = gtidSetDump(gtid_master);
        sds gtid_executed_repr = gtidSetDump(server.gtid_executed);
        sds gtid_lost_repr = gtidSetDump(server.gtid_lost);
        serverLog(LL_NOTICE,
                "%s gtid.set-master(%s) = gtid.set-executed(%s) + gtid.set-lost(%s)",
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
        for (int i=1, len=GTID_COMMAN_ARGC + 1; i < len && sdslen(args) < 128; i++) {
            args = sdscatprintf(args, "`%.*s`, ", 128-(int)sdslen(args), (char*)c->argv[i]->ptr);
        }
        addReplyErrorFormat(c,"gtid command already executed, %s",args);
        if(isGtidExecCommand(c)) {
            discardTransaction(c);
        }
        server.gtid_ignored_cmd_count++;
        if (server.masterhost) {
            serverLog(LL_WARNING, "gtid command already execute, %s", args);
            //TODO don't panic on GA
            serverPanic("gtid command already executed, %s", args);
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
    } else if ((c->cmd->arity > 0 && c->cmd->arity != c->argc) ||
               (c->argc < -c->cmd->arity)) {
        serverLog(LL_WARNING,"wrong number of arguments for '%s' command",
            c->cmd->name);
        rejectCommandFormat(c,"wrong number of arguments for '%s' command",
            c->cmd->name);
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

sds dumpServerReplMode() {
    return sdscatprintf(sdsempty(),"{(%s:%lld),(%s:%lld)}",
            replModeName(server.repl_mode->mode),
            server.repl_mode->from,
            replModeName(server.prev_repl_mode->mode),
            server.prev_repl_mode->from);
}

void resetServerReplModeFrom(int mode, long long from, const char *msg) {
    sds prev, cur;
    prev = dumpServerReplMode();
    replModeInit(server.prev_repl_mode);
    server.repl_mode->mode = mode;
    server.repl_mode->from = from;
    cur = dumpServerReplMode();
    serverLog(LL_NOTICE,"[gtid] reset repl mode to %s: %s => %s (%s)",
            replModeName(mode), prev, cur, msg);
    xsyncUuidInterestedInit();
    sdsfree(prev), sdsfree(cur);
}

void resetServerReplMode(int mode, const char *msg) {
    resetServerReplModeFrom(mode,server.master_repl_offset+1,msg);
}

void shiftServerReplModeFrom(int mode, long long from, const char *msg) {
    sds prev, cur;
    serverAssert(mode != REPL_MODE_UNSET && server.repl_mode->mode != mode);
    prev = dumpServerReplMode();
    if (server.repl_mode->from != from) {
        server.prev_repl_mode->from = server.repl_mode->from;
        server.prev_repl_mode->mode = server.repl_mode->mode;
    }
    server.repl_mode->from = from;
    server.repl_mode->mode = mode;

    cur = dumpServerReplMode();
    serverLog(LL_NOTICE,"[gtid] shift repl mode to %s: %s => %s (%s)",
            replModeName(mode), prev, cur, msg);
    xsyncUuidInterestedInit();
    sdsfree(prev), sdsfree(cur);
}

void shiftServerReplMode(int mode, const char *msg) {
    shiftServerReplModeFrom(mode,server.master_repl_offset+1,msg);
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

#define GTID_AUX_REPL_MODE    "gtid-repl-mode"
#define GTID_AUX_EXECUTED     "gtid-executed"
#define GTID_AUX_LOST         "gtid-lost"
#define GTID_AUX_MASTER_UUID  "gtid-master-uuid"

int rdbSaveGtidInfoAuxFields(rio* rdb) {
    char *repl_mode = (char*)replModeName(server.repl_mode->mode);
    sds gtid_executed_repr = gtidSetDump(server.gtid_executed);
    sds gtid_lost_repr = gtidSetDump(server.gtid_lost);
    size_t master_uuid_len;
    const char *master_uuid = getMasterUuid(&master_uuid_len);

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

    if (rdbSaveAuxField(rdb, GTID_AUX_MASTER_UUID,
                strlen(GTID_AUX_MASTER_UUID),
                (void*)master_uuid, master_uuid_len) == -1) {
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
        int repl_mode;
        char msg[64];
        if (!strcasecmp(val->ptr, "xsync")) {
            repl_mode = REPL_MODE_XSYNC;
        } else if (!strcasecmp(val->ptr, "psync")) {
            repl_mode = REPL_MODE_PSYNC;
        } else {
            repl_mode = REPL_MODE_PSYNC;
        }
        snprintf(msg,sizeof(msg),"auxload %s=%s",(sds)key->ptr,(sds)val->ptr);
        resetServerReplMode(repl_mode,msg);
        return 1;
    } else if (!strcasecmp(key->ptr, GTID_AUX_EXECUTED)) {
        serverGtidSetResetExecuted(gtidSetDecode(val->ptr,sdslen(val->ptr)));
        return 1;
    } else if (!strcasecmp(key->ptr, GTID_AUX_LOST)) {
        gtidSet *gtid_lost;
        if (!(gtid_lost = gtidSetDecode(val->ptr, sdslen(val->ptr))))
            gtid_lost = gtidSetNew();
        serverGtidSetResetLost(gtid_lost);
        return 1;
    } else if (!strcasecmp(key->ptr, GTID_AUX_MASTER_UUID)) {
        char *master_uuid = val->ptr;
        size_t master_uuid_len = sdslen(val->ptr);
        if (master_uuid_len == CONFIG_RUN_ID_SIZE) {
            memcpy(server.master_uuid,val->ptr,CONFIG_RUN_ID_SIZE);
            server.master_uuid[CONFIG_RUN_ID_SIZE] = 0;
        } else {
            serverLog(LL_WARNING, "[gtid] aux load %s size invalid(%s:%ld)",
                    GTID_AUX_MASTER_UUID,master_uuid,master_uuid_len);
        }
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

void shiftReplStreamIfNeeded(int mode, int flags, char *cause) {
    char msg[64];
    int cur_mode = server.repl_mode->mode;
    /* No need to shift if cur and next mode match. */
    if (cur_mode == mode) return;
    snprintf(msg,sizeof(msg),"shift repl stream %s => %s: %s",
            replModeName(cur_mode), replModeName(mode), cause);
    shiftServerReplMode(mode,msg);
    if (mode == REPL_MODE_PSYNC && server.masterhost == NULL) {
        /* set slaveseldb to -1 so that repl stream will re-select db */
        server.slaveseldb = -1;
        changeReplicationId();
        clearReplicationId2();
    }
    if (flags & GTID_SHIFT_REPL_STREAM_DISCARD_CACHED_MASTER)
        replicationDiscardCachedMaster();
    if (flags & GTID_SHIFT_REPL_STREAM_NOTIFY_SLAVES) {
        serverLog(LL_NOTICE, "[gtid] Disconnect slaves to notify: %s", msg);
        disconnectSlaves();
    }
}

void resetReplStreamPsync(char *replid, long long offset, char *cause) {
    char msg[128];

    memcpy(server.replid,replid,sizeof(server.replid));
    snprintf(msg,sizeof(msg),"repl stream reset to psync(%s:%lld): %s",
            replid, offset, cause);

    if (server.master_repl_offset == offset) {
        shiftReplStreamIfNeeded(REPL_MODE_PSYNC,
                GTID_SHIFT_REPL_STREAM_FULL,msg);
        return;
    }

    serverLog(LL_NOTICE,
            "[gtid] master repl offset bumped from %lld to %lld: %s",
            server.master_repl_offset, offset, msg);
    server.master_repl_offset = offset;

    /* backlog became invalid when master repl offset bumped */
    ctrip_resetReplicationBacklog();

    /* logically treat [1 ~ master_reploffset] as xsync */
    resetServerReplModeFrom(REPL_MODE_XSYNC,1,msg);
    shiftServerReplMode(REPL_MODE_PSYNC,msg);

    replicationDiscardCachedMaster();
    serverLog(LL_NOTICE, "[gtid] Disconnect slaves to notify %s.", msg);
    disconnectSlaves();
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
            gtidSet *gtid_slave;
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
        gtidSetFree(request->x.gtid_slave);
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

void masterParseXsyncRequest(syncRequest *request, robj *uuid, robj *gtidset, int optargc,
        robj **optargv) {
    long long maxgap;
    gtidSet *gtid_slave;
    sds gtid_repr = gtidset->ptr;

    if (!strcmp(uuid->ptr,GTID_XSYNC_UUID_INTERESTED_FULLRESYNC)) {
        request->mode = REPL_MODE_UNSET;
        request->i.msg = sdsnew("fullresync requested");
        return;
    }

    if ((gtid_slave = gtidSetDecode(gtid_repr,sdslen(gtid_repr))) == NULL) {
        request->mode = REPL_MODE_UNSET;
        request->i.msg = sdscatprintf(sdsempty(), "invalid gtid.set %s", gtid_repr);
        return;
    }

    request->mode = REPL_MODE_XSYNC;
    request->x.gtid_slave = gtid_slave;
    for (int i = 0; i+1 < optargc; i += 2) {
        if (!strcasecmp(optargv[i]->ptr,"maxgap")) {
            if (getLongLongFromObject(optargv[i+1],&maxgap) != C_OK) {
                maxgap = 0;
                serverLog(LL_NOTICE, "Ignored invalid maxgap option: %s",
                        (sds)optargv[i+1]->ptr);
            }
        }
    }
    request->x.maxgap = maxgap;
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


#define SYNC_ACTION_NOP         0
#define SYNC_ACTION_XCONTINUE   1
#define SYNC_ACTION_CONTINUE    2
#define SYNC_ACTION_FULL        3

/*  -1: no need to align offset
 * >=0: align offset by CONTINUE reply <cc.offset> */
#define SYNC_CONTINUE_OFFSET_NOALIGN -1

typedef struct syncResult {
    int request_mode;
    int action;
    long long offset; /* send offset start */
    long long limit; /* send offset limit */
    sds msg;
    union {
        struct {
            gtidSet *gtid_cont;
            gtidSet *gtid_lost;
        } xc; /* xcontinue */
        struct {
            sds replid;
            /* offset that slave has logically received. will be propagate
             * to slave when xcontinue => continue to align repl offset
             * of master and slave.
             * Note that to keep compatible with origin replication proptocol,
             * when offset < 0, it will not be propragte to slave. */
            long long offset;
            /* gtid.set-lost might exists when xsync => psync. */
            gtidSet *gtid_lost;
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
        gtidSetFree(result->xc.gtid_cont);
        gtidSetFree(result->xc.gtid_lost);
        break;
    case SYNC_ACTION_CONTINUE:
        sdsfree(result->cc.replid);
        gtidSetFree(result->cc.gtid_lost);
        break;
    case SYNC_ACTION_FULL:
        break;
    }
    sdsfree(result->msg);
    zfree(result);
}

void masterAnaPsyncRequest(syncResult *result, syncRequest *request) {
    syncLocateResult slr;
    sds master_replid = request->p.replid;
    long long psync_offset = request->p.offset;

    if (!server.repl_backlog ||
        psync_offset < server.repl_backlog_off ||
        psync_offset > (server.repl_backlog_off+server.repl_backlog_histlen)) {
        result->action = SYNC_ACTION_FULL;
        result->msg = sdscatprintf(sdsempty(),
                "psync offset(%lld) not in backlog [%lld,%lld)",
                psync_offset, server.repl_backlog_off,
                server.repl_backlog_off+server.repl_backlog_histlen);
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
        if ((!strcasecmp(master_replid, server.replid) &&
                    psync_offset <= server.repl_mode->from) ||
                (!strcasecmp(master_replid, server.replid2) &&
                 psync_offset <= server.second_replid_offset)) {
            if (slr.locate_type == LOCATE_TYPE_PREV) {
                result->action = SYNC_ACTION_CONTINUE;
                result->limit = slr.p.limit;
                result->cc.replid = sdsnew(server.replid);
                result->cc.offset = SYNC_CONTINUE_OFFSET_NOALIGN;
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
                serverLog(LL_NOTICE, "[psync] gtid.set-continue(%s) = gtid.set-master(%s) - gtid.set-xsync(%s)", gtid_continue_repr,gtid_master_repr,gtid_xsync_repr);

                result->action = SYNC_ACTION_XCONTINUE;
                result->xc.gtid_cont = gtid_cont, gtid_cont = NULL;
                result->xc.gtid_lost = gtidSetNew();
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
                    master_replid, psync_offset,
                    server.replid, server.repl_mode->from,
                    server.replid2, server.second_replid_offset);
        }
    } else {
        /* Let origin redis hanle this psync request */
        result->action = SYNC_ACTION_NOP;
    }

    syncLocateResultDeinit(&slr);
}

void masterAnaXsyncRequest(syncResult *result, syncRequest *request) {
    syncLocateResult slr;
    long long psync_offset, maxgap = request->x.maxgap;
    gtidSet *gtid_slave = request->x.gtid_slave;
    gtidSet *gtid_master = NULL, *gtid_cont = NULL, *gtid_xsync = NULL,
            *gtid_gap = NULL, *gtid_mlost = NULL, *gtid_slost = NULL;
    sds gtid_master_repr = NULL, gtid_continue_repr = NULL,
        gtid_xsync_repr = NULL, gtid_slave_repr = NULL,
        gtid_mlost_repr = NULL, gtid_slost_repr = NULL;

    syncLocateResultInit(&slr);

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
        serverLog(LL_NOTICE, "[xsync] [ana] continue point defaults to backlog tail: gtid.seq not exists.");
    } else {
        psync_offset = gtidSeqXsync(server.gtid_seq,gtid_slave,&gtid_xsync);
    }

    gtid_xsync_repr = gtidSetDump(gtid_xsync);
    serverLog(LL_NOTICE, "[xsync] [ana] continue point locate at offset=%lld, gtid.set-xsync=%s", psync_offset, gtid_xsync_repr);

    if (psync_offset < 0) {
        if (server.repl_mode->mode == REPL_MODE_XSYNC) {
            psync_offset = server.master_repl_offset+1;
            serverLog(LL_NOTICE, "[xsync] [ana] continue point adjust to backlog tail: offset=%lld", psync_offset);
        } else {
            psync_offset = server.repl_mode->from;
            serverLog(LL_NOTICE, "[xsync] [ana] continue point adjust to psync from: offset=%lld", psync_offset);
        }
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
    serverLog(LL_NOTICE, "[xsync] [ana] gtid.set-continue(%s) = gtid.set-master(%s) - gtid.set-xsync(%s)", gtid_continue_repr,gtid_master_repr,gtid_xsync_repr);

    gtid_slost = gtidSetDup(gtid_cont);
    gtidSetDiff(gtid_slost,gtid_slave);

    gtid_slost_repr = gtidSetDump(gtid_slost);
    serverLog(LL_NOTICE, "[xsync] [ana] gtid.set-slost(%s) = gtid.set-continue(%s) - gtid.set-slave(%s)", gtid_slost_repr,gtid_continue_repr,gtid_slave_repr);

    gtid_mlost = gtidSetDup(gtid_slave);
    gtidSetDiff(gtid_mlost,gtid_cont);

    gtid_mlost_repr = gtidSetDump(gtid_mlost);
    serverLog(LL_NOTICE, "[xsync] [ana] gtid.set-mlost(%s) = gtid.set-slave(%s) - gtid.set-continue(%s)", gtid_mlost_repr,gtid_slave_repr,gtid_continue_repr);

    gno_t gap = gtidSetCount(gtid_mlost) + gtidSetCount(gtid_slost);
    if (gap > maxgap) {
        result->action = SYNC_ACTION_FULL;
        result->msg = sdscatprintf(sdsempty(), "gap=%lld > maxgap=%lld",
                gap, maxgap);
        goto end;
    }

    if (slr.locate_type == LOCATE_TYPE_PREV) {
        result->action = SYNC_ACTION_XCONTINUE;
        result->limit = slr.p.limit;
        result->xc.gtid_cont = gtid_cont, gtid_cont = NULL;
        result->xc.gtid_lost = gtid_mlost, gtid_mlost = NULL;
        result->msg = sdscatprintf(sdsempty(),
                "gap=%lld <= maxgap=%lld",gap,maxgap);
    } else if (slr.locate_type == LOCATE_TYPE_SWITCH) {
        result->action = SYNC_ACTION_CONTINUE;
        result->cc.replid = sdsnew(server.replid);
        /* logically, offset (.. psync_offset-1] has already received */
        result->cc.offset = psync_offset-1;
        result->cc.gtid_lost = gtid_mlost, gtid_mlost = NULL;
        result->msg = sdsnew("xsync => psync");
    } else {
        result->action = SYNC_ACTION_XCONTINUE;
        result->xc.gtid_cont = gtid_cont, gtid_cont = NULL;
        result->xc.gtid_lost = gtid_mlost, gtid_mlost = NULL;
        result->msg = sdscatprintf(sdsempty(),
                "gap=%lld <= maxgap=%lld",gap,maxgap);
    }

end:
    syncLocateResultDeinit(&slr);

    sdsfree(gtid_master_repr), sdsfree(gtid_continue_repr);
    sdsfree(gtid_xsync_repr), sdsfree(gtid_slave_repr);
    sdsfree(gtid_mlost_repr), sdsfree(gtid_slost_repr);

    gtidSetFree(gtid_master), gtidSetFree(gtid_cont), gtidSetFree(gtid_xsync);
    gtidSetFree(gtid_gap), gtidSetFree(gtid_mlost), gtidSetFree(gtid_slost);
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

typedef void (*consume_cb)(char *p, long long thislen, void *pd);

/* Check adReplyReplicationBacklog for more details */
long long consumeReplicationBacklogLimited(long long offset, long long limit,
         consume_cb cb, void *pd) {
    long long added = 0, j, skip, len;
    serverAssert(limit >= 0 && offset >= server.repl_backlog_off);
    if (server.repl_backlog_histlen == 0) return 0;
    skip = offset - server.repl_backlog_off;
    j = (server.repl_backlog_idx +
        (server.repl_backlog_size-server.repl_backlog_histlen)) %
        server.repl_backlog_size;
    j = (j + skip) % server.repl_backlog_size;
    len = server.repl_backlog_histlen - skip;
    len = len < limit ? len : limit; /* limit bytes to copy */
    while(len) {
        long long thislen =
            ((server.repl_backlog_size - j) < len) ?
            (server.repl_backlog_size - j) : len;
        cb(server.repl_backlog + j, thislen, pd);
        len -= thislen;
        j = 0;
        added += thislen;
    }

    return added;
}

static void consumeReplicationBacklogLimitedAddReplyCb(char *p,
        long long thislen, void *pd) {
    addReplySds((client*)pd, sdsnewlen(p,thislen));
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
    c->repl_put_online_on_ack = 0;
    listAddNodeTail(server.slaves,c);

    if (connWrite(c->conn,buf,buflen) != buflen) {
        freeClientAsync(c);
        return;
    }

    if (limit > 0) {
        sent = addReplyReplicationBacklogLimited(c,offset,limit);
    } else {
        sent = addReplyReplicationBacklog(c,offset);
    }

    serverLog(LL_NOTICE,
        "[gtid] Sent %lld bytes of backlog starting from offset %lld limit %lld.",
        sent, offset, limit);

    if (limit > 0) {
        c->flags |= CLIENT_CLOSE_AFTER_REPLY;
        serverLog(LL_NOTICE,
                "[gtid] Disconnect slave %s to notify repl mode switched.",
                replicationGetSlaveName(c));
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

/* See disconnectSlaves for more details */
static void disconnectSlavesExcept(client *trigger_slave) {
    listIter li;
    listNode *ln;
    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = (client *)ln->value;
        if (slave == trigger_slave) continue;
        sds client_desc = catClientInfoString(sdsempty(), slave);
        serverLog(LL_NOTICE,"[gtid] Disconnect slave to notify gtid.set-lost update: %s", client_desc);
        writeToClient(slave,0);
        if (clientHasPendingReplies(slave))
            serverLog(LL_NOTICE, "[gtid] Slave still have pending replies when disconnect: %s", client_desc);
        freeClient(slave);
        sdsfree(client_desc);
    }
}

/* trigger_slave is NULL if server.gtid_lost update triggered by master. */
static void serverUpdateGtidLost(gtidSet *delta_lost, client *trigger_slave) {
    sds gtid_lost_repr = NULL, gtid_delta_lost_repr = NULL, gtid_updated_lost_repr = NULL;
    int from_master = trigger_slave == NULL;

    if (gtidSetCount(delta_lost) == 0) return;

    gtid_lost_repr = gtidSetDump(server.gtid_lost);
    gtid_delta_lost_repr = gtidSetDump(delta_lost);

    serverGtidSetAddLost(delta_lost);

    gtid_updated_lost_repr = gtidSetDump(server.gtid_lost);
    serverLog(LL_NOTICE, "[gtid] gtid.set-lost update: gtid.set-lost(%s) = gtid.set-lost(%s) + gtid.set-delta_lost(%s)", gtid_updated_lost_repr, gtid_lost_repr, gtid_delta_lost_repr);

    if (server.masterhost && !from_master) {
        serverLog(LL_NOTICE, "[gtid] reconnect with master to sync gtid.set-lost update");
        if (server.master) freeClientAsync(server.master);
        cancelReplicationHandshake(0);
    }

    if (listLength(server.slaves)) {
        serverLog(LL_NOTICE, "[gtid] disconnect slaves to sync gtid.set-lost update");
        disconnectSlavesExcept(trigger_slave);
    }

    sdsfree(gtid_lost_repr), sdsfree(gtid_delta_lost_repr);
    sdsfree(gtid_updated_lost_repr);
}

int masterReplySyncRequest(client *c, syncResult *result) {
    int ret = result->action == SYNC_ACTION_FULL ? C_ERR : C_OK;

    if (result->action == SYNC_ACTION_NOP) {
        serverLog(LL_NOTICE, "[%s] Partial sync request from %s handle by vanilla redis.", replModeName(result->request_mode), replicationGetSlaveName(c));
        ret = masterTryPartialResynchronization(c);
    } else if (result->action == SYNC_ACTION_XCONTINUE) {
        char *buf;
        int buflen;
        const char *master_uuid;
        size_t master_uuid_len;
        sds gtid_cont_repr = gtidSetDump(result->xc.gtid_cont);

        serverLog(LL_NOTICE, "[%s] Partial sync request from %s accepted: %s, offset=%lld, limit=%lld, gtid.set-cont=%s, master.uuid=%s", replModeName(result->request_mode), replicationGetSlaveName(c), result->msg, result->offset, result->limit, gtid_cont_repr, server.uuid);

        if (result->xc.gtid_lost) serverUpdateGtidLost(result->xc.gtid_lost,c);

        master_uuid = getMasterUuid(&master_uuid_len);

        buflen = sdslen(gtid_cont_repr)+256;
        buf = zcalloc(buflen);
        buflen = snprintf(buf,buflen,
                "+XCONTINUE GTID.SET %.*s MASTER.UUID %.*s\r\n",
                (int)sdslen(gtid_cont_repr),gtid_cont_repr,
                (int)master_uuid_len,master_uuid);
        masterSetupPartialSynchronization(c,result->offset,
                result->limit,buf,buflen);

        sdsfree(gtid_cont_repr);
        zfree(buf);
    } else if (result->action == SYNC_ACTION_CONTINUE) {
        char buf[128];
        int buflen;

        serverLog(LL_NOTICE, "[%s] Partial sync request from %s accepted: %s, offset=%lld, limit=%lld, cc.replid=%s, cc.offset=%lld", replModeName(result->request_mode), replicationGetSlaveName(c), result->msg, result->offset, result->limit, result->cc.replid, result->cc.offset);

        if (result->cc.gtid_lost) serverUpdateGtidLost(result->cc.gtid_lost,c);

        if (result->cc.offset == SYNC_CONTINUE_OFFSET_NOALIGN) {
            buflen = snprintf(buf,sizeof(buf),"+CONTINUE %s\r\n",
                    result->cc.replid);
        } else {
            buflen = snprintf(buf,sizeof(buf),"+CONTINUE %s %lld\r\n",
                    result->cc.replid, result->cc.offset);
        }
        masterSetupPartialSynchronization(c,result->offset,
                result->limit,buf,buflen);
    } else {
        serverLog(LL_NOTICE, "[%s] Partial sync request from %s rejected: %s", replModeName(result->request_mode), replicationGetSlaveName(c), result->msg);
    }

    return ret;
}

int ctrip_masterTryPartialResynchronization(client *c) {
    syncRequest *request = masterParseSyncRequest(c);
    syncResult *result = masterAnaSyncRequest(request);
    int ret = masterReplySyncRequest(c,result);
    syncRequestFree(request);
    syncResultFree(result);
    return ret;
}

const char *xsyncUuidInterestedGet(void);

int ctrip_slaveTryPartialResynchronizationWrite(connection *conn) {
    gtidSet *gtid_slave = NULL;
    char maxgap[32];
    int result = PSYNC_WAIT_REPLY;
    sds gtid_slave_repr;

    serverLog(LL_NOTICE, "[gtid] Trying parital sync in (%s) mode.",
            replModeName(server.repl_mode->mode));

    if (server.repl_mode->mode != REPL_MODE_XSYNC) return -1;

    /* Explicitly clear master_replid/master_initial_offset to avoid any
     * further usage. Note that offset was intentionally set to 0 to avoid
     * flagging master client as CLIENT_PRE_PSYNC. */
    memset(server.master_replid,'0',sizeof(server.master_replid));
    server.master_replid[CONFIG_RUN_ID_SIZE] = '\0';
    server.master_initial_offset = 0;

    gtid_slave = serverGtidSetGet("[xsync]");
    gtid_slave_repr = gtidSetDump(gtid_slave);
    const char *uuid_interested = xsyncUuidInterestedGet();

    snprintf(maxgap,sizeof(maxgap),"%lld",server.gtid_xsync_max_gap);
    serverLog(LL_NOTICE, "[xsync] Trying partial xsync with uuid_interested=%s, gtid.set=%s, maxgap=%s",uuid_interested,gtid_slave_repr,maxgap);

    sds reply = sendCommand(conn,"XSYNC",uuid_interested,
            gtid_slave_repr,"MAXGAP",maxgap,NULL);
    if (reply != NULL) {
        serverLog(LL_WARNING,"[xsync] Unable to send XSYNC: %s", reply);
        sdsfree(reply);
        connSetReadHandler(conn, NULL);
        result = PSYNC_WRITE_ERROR;
    }

    gtidSetFree(gtid_slave);
    sdsfree(gtid_slave_repr);

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
                    "[psync] repl mode switch: psync => xsync (xfullresync)");
            /* Repl mode will be reset on rdb loading */
            replicationDiscardCachedMaster();
            sdsfree(reply);
            server.gtid_sync_stat[GTID_SYNC_PSYNC_XFULLRESYNC]++;
            return PSYNC_FULLRESYNC;
        }

        if (!strncmp(reply,"+XCONTINUE",10)) {
            /* PSYNC => XCONTINUE */
            serverLog(LL_NOTICE,
                    "[psync] repl mode switch: psync => xsync (xcontinue)");

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
                        serverLog(LL_WARNING, "[psync] Parsed invalid gtid.set-cont(%s)", tokens[i+1]);
                        result = PSYNC_TRY_LATER;
                        break;
                    }

                    gtid_slave = serverGtidSetGet("[psync]");
                    if (!gtidSetEqual(gtid_slave,gtid_cont)) {
                        sds gtid_slave_repr = gtidSetDump(gtid_slave);
                        serverLog(LL_WARNING,"[psync] master reply with xcontinue, but gtid.set-cont(%s) != gtid.set-slave(%s).",gtid_slave_repr, tokens[i+1]);
                        sdsfree(gtid_slave_repr);
                        result = PSYNC_TRY_LATER;
                    }
                    gtidSetFree(gtid_cont);
                    gtidSetFree(gtid_slave);
                } else if (!strncasecmp(tokens[i], "master.uuid", sdslen(tokens[i]))) {
                    const char *master_uuid = tokens[i+1];
                    size_t master_uuid_len = sdslen(tokens[i+1]);
                    if (master_uuid_len != CONFIG_RUN_ID_SIZE) {
                        serverLog(LL_WARNING, "[psync] Parsed invalid master.uuid(%s)", tokens[i+1]);
                        result = PSYNC_TRY_LATER;
                        break;
                    }
                    memcpy(server.master_uuid,master_uuid,CONFIG_RUN_ID_SIZE);
                    server.master_uuid[CONFIG_RUN_ID_SIZE] = 0;
                    server.master_uuid_len = master_uuid_len;
                } else {
                    serverLog(LL_NOTICE,
                            "[psync] Ignored xcontinue option: %s", tokens[i]);
                }
            }

            if (result == PSYNC_CONTINUE) {
                shiftReplStreamIfNeeded(REPL_MODE_XSYNC,
                        GTID_SHIFT_REPL_STREAM_FULL,"slave psync=>xcontinue");
                replicationCreateMasterClient(conn,-1);
                ctrip_replicationMasterLinkUp();
                if (server.repl_backlog == NULL)
                    ctrip_createReplicationBacklog();
            } else {
                serverLog(LL_WARNING, "[psync] Discard cached master to force fullresync");
                replicationDiscardCachedMaster();
            }

            sdsfreesplitres(tokens,ntoken);
            sdsfree(reply);
            server.gtid_sync_stat[GTID_SYNC_PSYNC_XCONTINUE]++;
            return result;
        }

        if (!strncmp(reply,"+FULLRESYNC",11)) {
            server.gtid_sync_stat[GTID_SYNC_PSYNC_FULLRESYNC]++;
        }
        if (!strncmp(reply,"+CONTINUE",9)) {
            server.gtid_sync_stat[GTID_SYNC_PSYNC_CONTINUE]++;
        }

        /* psync => fullresync, psync => continue, unknown response handled
         * by origin psync. */
        return -1;
    } else {
        if (!strncmp(reply,"+FULLRESYNC",11)) {
            /* XSYNC => FULLRESYNC */
            serverLog(LL_NOTICE,
                    "[xsync] repl mode switch: xsync => psync (fullresync)");
            /* Repl mode will be reset on rdb loading */
            server.gtid_sync_stat[GTID_SYNC_XSYNC_FULLRESYNC]++;
            return -1;
        }

        if (!strncmp(reply,"+CONTINUE",9)) {
            /* XSYNC => CONTINUE */
            serverLog(LL_NOTICE,
                    "[xsync] repl mode switch: xsync => psync (continue)");
            serverAssert(!server.master && !server.cached_master);

            server.gtid_sync_stat[GTID_SYNC_XSYNC_CONTINUE]++;

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
                return PSYNC_NOT_SUPPORTED;
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
                return PSYNC_NOT_SUPPORTED;
            }

            /* Create master client with args specified in CONTINUE reply. */
            memcpy(server.master_replid,new,sizeof(server.master_replid));
            server.master_initial_offset = offset;
            replicationCreateMasterClient(conn,-1);
            ctrip_replicationMasterLinkUp();

            /* Align replid and offset with master since in psync mode now. */
            resetReplStreamPsync(new,offset,"slave xsync=>continue");

            sdsfree(reply);
            return PSYNC_CONTINUE;
        }

        if (!strncmp(reply,"+XFULLRESYNC",12)) {
            /* XSYNC => XFULLRESYNC */
            serverLog(LL_NOTICE,"[xsync] XFullResync from master: %s.", reply);
            serverAssert(!server.cached_master && !server.master);
            sdsfree(reply);
            server.gtid_sync_stat[GTID_SYNC_XSYNC_XFULLRESYNC]++;
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
                    sds gtid_cont_repr, gtid_slave_repr, gtid_slost_repr;

                    gtid_cont = gtidSetDecode(tokens[i+1],sdslen(tokens[i+1]));
                    if (gtid_cont == NULL) {
                        serverLog(LL_WARNING, "[xsync] invalid gtid.set-cont(%s)", tokens[i+1]);
                        result = PSYNC_NOT_SUPPORTED;
                        break;
                    }

                    gtid_slave = serverGtidSetGet("[xsync]");
                    gtid_slave_repr = gtidSetDump(gtid_slave);

                    gtid_slost = gtidSetDup(gtid_cont);
                    gtidSetDiff(gtid_slost,gtid_slave);

                    gtid_cont_repr = gtidSetDump(gtid_cont);
                    gtid_slost_repr = gtidSetDump(gtid_slost);
                    serverLog(LL_NOTICE, "[xsync] gtid.set-slost(%s) = gtid.set-continue(%s) - gtid.set-slave(%s)", gtid_slost_repr,gtid_cont_repr,gtid_slave_repr);

                    serverUpdateGtidLost(gtid_slost,NULL);

                    sdsfree(gtid_cont_repr), sdsfree(gtid_slave_repr), sdsfree(gtid_slost_repr);

                    gtidSetFree(gtid_cont), gtidSetFree(gtid_slost);
                    gtidSetFree(gtid_slave);
                } else if (!strncasecmp(tokens[i], "master.uuid", sdslen(tokens[i]))) {
                    const char *master_uuid = tokens[i+1];
                    size_t master_uuid_len = sdslen(tokens[i+1]);
                    if (master_uuid_len != CONFIG_RUN_ID_SIZE) {
                        serverLog(LL_WARNING, "[psync] Parsed invalid master.uuid(%s)", tokens[i+1]);
                        result = PSYNC_TRY_LATER;
                        break;
                    }
                    memcpy(server.master_uuid,master_uuid,CONFIG_RUN_ID_SIZE);
                    server.master_uuid[CONFIG_RUN_ID_SIZE] = 0;
                    server.master_uuid_len = master_uuid_len;
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

            server.gtid_sync_stat[GTID_SYNC_XSYNC_XCONTINUE]++;

            sdsfree(reply);
            sdsfreesplitres(tokens,ntoken);
            return result;
        }

        /* unknown response handled by origin psync. */
        return -1;
    }
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

void ctrip_afterErrorReply(client *c, const char *s, size_t len) {
    afterErrorReply(c,s,len);
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
    disconnectSlaves();
}

void xsyncReplicationCron() {
    forceXsyncFullResyncIfNeeded();
}

static long long getPsyncSlaveReplOff() {
    long long slave_repl_offset;
    if (server.master) {
        slave_repl_offset = server.master->reploff;
    } else if (server.cached_master) {
        slave_repl_offset = server.cached_master->reploff;
    } else {
        slave_repl_offset = 1;
    }
    return slave_repl_offset;
}

/* In order to help sentinel choose the newest slave to failover
 * we use applied master gno to mock slave_repl_offset, while
 * the origin offset are exposed by gtid_psync_slave_repl_offset. */
long long ctrip_getSlaveReplOff() {
    long long slave_repl_offset;
    if (server.repl_mode->mode == REPL_MODE_XSYNC) {
        gtidSet *gtid_set;
        size_t uuid_len;
        const char *uuid = getMasterUuid(&uuid_len);
        gtid_set = serverGtidSetGet(NULL);
        slave_repl_offset = gtidSetNext(gtid_set,uuid,uuid_len,0);
        gtidSetFree(gtid_set);
    } else {
        slave_repl_offset = getPsyncSlaveReplOff();
    }
    return slave_repl_offset;
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
            "gtid_prev_repl_mode:%s\r\n"
            "gtid_prev_repl_from:%lld\r\n"
            "gtid_uuid_interested:%s\r\n"
            "gtid_xsync_fullresync_indicator:%lld\r\n"
            "gtid_psync_slave_repl_offset:%lld\r\n"
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
            replModeName(server.prev_repl_mode->mode),
            server.prev_repl_mode->from,
            server.gtid_uuid_interested,
            server.gtid_xsync_fullresync_indicator,
            getPsyncSlaveReplOff(),
            server.gtid_executed_cmd_count,
            server.gtid_ignored_cmd_count);

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

#ifdef REDIS_TEST
int gtidTest(int argc, char **argv, int accurate) {
    UNUSED(argc), UNUSED(argv), UNUSED(accurate);

    int error = 0;
    server.maxmemory_policy = MAXMEMORY_FLAG_LFU;
    if (!server.logfile) server.logfile = zstrdup(CONFIG_DEFAULT_LOGFILE);

    TEST("gtid - parse xsync request") {
        syncRequest *request = syncRequestNew();
        robj *optargv[2];
        robj *gtidset = createStringObject("A:1-100,B", 9);
        robj *uuid_interested = createStringObject("*",1);
        optargv[0] = createStringObject("MAXGAP",6);
        optargv[1] = createStringObject("10000", 5);
        masterParseXsyncRequest(request,uuid_interested,gtidset,2,optargv);
        test_assert(request->mode == REPL_MODE_XSYNC);
        test_assert(request->x.maxgap == 10000);
        test_assert(gtidSetCount(request->x.gtid_slave) == 100);
        decrRefCount(optargv[0]);
        decrRefCount(optargv[1]);
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

        locateServerReplMode(REPL_MODE_XSYNC,3000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_CUR);
        test_assert(slr->locate_mode == REPL_MODE_XSYNC);

        locateServerReplMode(REPL_MODE_XSYNC,2000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_CUR);
        test_assert(slr->locate_mode == REPL_MODE_XSYNC);

        locateServerReplMode(REPL_MODE_PSYNC,2000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_SWITCH);
        test_assert(slr->locate_mode == REPL_MODE_XSYNC);

        locateServerReplMode(REPL_MODE_XSYNC,1000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_INVALID);
        syncLocateResultDeinit(slr);

        locateServerReplMode(REPL_MODE_PSYNC,1000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_PREV);
        test_assert(slr->locate_mode == REPL_MODE_PSYNC);
        test_assert(slr->p.limit == 1000);

        locateServerReplMode(REPL_MODE_XSYNC,500,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_INVALID);
        syncLocateResultDeinit(slr);

        /* {(psync:2000), (xsync:1000)}*/
        server.prev_repl_mode->from = 1000, server.prev_repl_mode->mode = REPL_MODE_XSYNC;
        server.repl_mode->from = 2000, server.repl_mode->mode = REPL_MODE_PSYNC;

        locateServerReplMode(REPL_MODE_PSYNC,3000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_CUR);
        test_assert(slr->locate_mode == REPL_MODE_PSYNC);

        locateServerReplMode(REPL_MODE_PSYNC,2000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_CUR);
        test_assert(slr->locate_mode == REPL_MODE_PSYNC);

        locateServerReplMode(REPL_MODE_XSYNC,2000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_SWITCH);
        test_assert(slr->locate_mode == REPL_MODE_PSYNC);

        locateServerReplMode(REPL_MODE_PSYNC,1000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_INVALID);
        syncLocateResultDeinit(slr);

        locateServerReplMode(REPL_MODE_XSYNC,1000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_PREV);
        test_assert(slr->locate_mode == REPL_MODE_XSYNC);
        test_assert(slr->p.limit == 1000);

        locateServerReplMode(REPL_MODE_XSYNC,500,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_INVALID);
        syncLocateResultDeinit(slr);

        /* {(xsync:2000), {?:1000}}*/
        server.prev_repl_mode->from = 1000, server.prev_repl_mode->mode = REPL_MODE_UNSET;
        server.repl_mode->from = 2000, server.repl_mode->mode = REPL_MODE_XSYNC;

        locateServerReplMode(REPL_MODE_XSYNC,3000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_CUR);
        test_assert(slr->locate_mode == REPL_MODE_XSYNC);

        locateServerReplMode(REPL_MODE_XSYNC,2000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_CUR);
        test_assert(slr->locate_mode == REPL_MODE_XSYNC);

        locateServerReplMode(REPL_MODE_PSYNC,2000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_INVALID);
        syncLocateResultDeinit(slr);

        locateServerReplMode(REPL_MODE_PSYNC,1000,slr);
        test_assert(slr->locate_type == LOCATE_TYPE_INVALID);

        syncLocateResultDeinit(slr);
    }

    return error;
}

#endif

