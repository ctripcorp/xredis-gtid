#include "server.h"
#include <gtid.h>
#define GTID_COMMAN_ARGC 3

int isGtidEnabled() {
    return server.gtid_enabled;
}


int isGtidExecCommand(client* c) {
    return c->cmd->proc == gtidCommand && c->argc > GTID_COMMAN_ARGC && strcasecmp(c->argv[GTID_COMMAN_ARGC]->ptr, "exec") == 0;
}

/* gtid.auto {comment} set k v => gtid {gtid_str} {dbid} {comment} */
void gtidAutoCommand(client* c) {
    if(strncmp(c->argv[1]->ptr, "/*", 2) != 0) {
        addReplyErrorFormat(c,"gtid.auto comment format error:%s", (char*)c->argv[1]->ptr);
        return;
    }
    int argc = c->argc;
    robj** argv = c->argv;
    struct redisCommand* cmd = c->cmd;
    c->argc = argc - 2;
    c->argv = argv + 2;
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
    server.dirty++;
end:
    c->argc = argc;
    c->argv = argv;
    c->cmd = cmd;
}


/**
 * @brief
 *      1. gtid A:1 {db} set k v
 *      2. gtid A:1 {db} exec
 *          a. fail (clean queue)
 *      3. gtid A:1 {db} \/\*comment\*\/ set k v
 *
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
            //clean multi queue
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

void propagateGtidExpire(redisDb *db, robj *key, int lazy) {
    int argc = 2 + GTID_COMMAN_ARGC;
    size_t maxlen = uuidSetEstimatedEncodeBufferSize(server.current_uuid);
    char *buf = zmalloc(maxlen);
    gno_t gno = uuidSetNext(server.current_uuid, 1);
    char *uuid = server.current_uuid->uuid;
    size_t uuid_len = server.current_uuid->uuid_len;
    size_t len = uuidGnoEncode(buf, maxlen, uuid, uuid_len, gno);

    robj *argv[argc];
    argv[0] = shared.gtid;
    argv[1] = createObject(OBJ_STRING, sdsnewlen(buf, len));
    argv[2] = createObject(OBJ_STRING, sdscatprintf(sdsempty(),
        "%d", db->id));

    argv[0 + GTID_COMMAN_ARGC] = lazy ? shared.unlink : shared.del;
    argv[1 + GTID_COMMAN_ARGC] = key;

    if (server.aof_state != AOF_OFF)
        feedAppendOnlyFile(server.delCommand,db->id,argv,argc);
    replicationFeedSlaves(server.slaves,db->id,argv,argc);

    zfree(buf);
    decrRefCount(argv[1]);
    decrRefCount(argv[2]);
}

int isGtidInMerge(client* c) {
    return c->gtid_in_merge;
}

/* set k v  -> gtid {gtid_str} set k v */
int execCommandPropagateGtid(struct redisCommand *cmd, int dbid, robj **argv, int argc,
               int flags) {
    if(!isGtidEnabled()) {
        return 0;
    }
    if (cmd == server.gtidCommand) {
        return 0;
    }

    if (cmd == server.gtidLwmCommand) {
        return 0;
    }

    if (cmd == server.gtidMergeStartCommand || cmd == server.gtidMergeEndCommand) {
        return 0;
    }

    if(server.in_exec && cmd != server.execCommand) {
        return 0;
    }

    if (cmd == server.multiCommand) {
        return 0;
    }
    robj *gtidArgv[argc+3];
    gtidArgv[0] = shared.gtid;
    size_t maxlen = uuidSetEstimatedEncodeBufferSize(server.current_uuid);
    char *buf = zmalloc(maxlen);
    char *uuid = server.current_uuid->uuid;
    size_t uuid_len = server.current_uuid->uuid_len;
    gno_t gno = uuidSetNext(server.current_uuid, 1);
    size_t len = uuidGnoEncode(buf, maxlen, uuid, uuid_len, gno);

    gtidArgv[1] = createObject(OBJ_STRING, sdsnewlen(buf, len));
    if (cmd == server.execCommand &&  server.db_at_multi != NULL) {
        gtidArgv[2] = createObject(OBJ_STRING, sdscatprintf(sdsempty(),
        "%d", server.db_at_multi->id));
    } else {
        gtidArgv[2] = createObject(OBJ_STRING, sdscatprintf(sdsempty(),
        "%d", dbid));
    }
    if(cmd == server.gtidAutoCommand) {
        for(int i = 0; i < argc-1; i++) {
            gtidArgv[i+3] = argv[i+1];
        }
        propagate(server.gtidCommand, dbid, gtidArgv, argc+2, flags);
    } else {
        for(int i = 0; i < argc; i++) {
            gtidArgv[i+3] = argv[i];
        }
        propagate(server.gtidCommand, dbid, gtidArgv, argc+3, flags);
    }
    zfree(buf);
    decrRefCount(gtidArgv[1]);
    decrRefCount(gtidArgv[2]);
    return 1;
}

/* gtid expireat command append buffer */
sds catAppendOnlyGtidExpireAtCommand(sds buf, robj* gtid, robj* dbid, robj* comment, struct redisCommand *cmd,  robj *key, robj *seconds) {
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
 * @brief
 *        gtid expire => gtid expireat
 *        gtid setex =>  set  + gtid expireat
 *        gtid set(ex) => set + gtid expireat
 */
sds gtidCommandTranslate(sds buf, struct redisCommand *cmd, robj **argv, int argc) {
    if(cmd == server.gtidCommand) {
        int index = 3;
        if(strncmp(argv[index]->ptr,"/*",2 ) == 0)  {
            index++;
        }
        struct redisCommand *c = lookupCommand(argv[index]->ptr);
        if (c->proc == expireCommand || c->proc == pexpireCommand ||
            c->proc == expireatCommand) {
            /* Translate EXPIRE/PEXPIRE/EXPIREAT into PEXPIREAT */
            if(index == 3) {
                buf = catAppendOnlyGtidExpireAtCommand(buf,argv[1],argv[2],NULL,c,argv[index+1],argv[index+2]);
            } else {
                buf = catAppendOnlyGtidExpireAtCommand(buf,argv[1],argv[2],argv[3],c,argv[index+1],argv[index+2]);
            }
        } else if (c->proc == setCommand && argc > 3 +  GTID_COMMAN_ARGC) {
            robj *pxarg = NULL;
            /* When SET is used with EX/PX argument setGenericCommand propagates them with PX millisecond argument.
            * So since the command arguments are re-written there, we can rely here on the index of PX being 3. */
            if (!strcasecmp(argv[3 + GTID_COMMAN_ARGC]->ptr, "px")) {
                pxarg = argv[4 + GTID_COMMAN_ARGC];
            }
            /* For AOF we convert SET key value relative time in milliseconds to SET key value absolute time in
            * millisecond. Whenever the condition is true it implies that original SET has been transformed
            * to SET PX with millisecond time argument so we do not need to worry about unit here.*/
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
    } else {
        buf = catAppendOnlyGenericCommand(buf,argc,argv);
    }
    return buf;
}


void gtidLwmCommand(client* c) {
    sds uuid = c->argv[1]->ptr;
    long long gno = 0;
    sds gno_str = c->argv[2]->ptr;
    if(string2ll(gno_str, sdslen(gno_str), &gno) == -1) {
        addReply(c, shared.err);
        return;
    }
    gtidSetRaise(server.gtid_executed,uuid, strlen(uuid), gno);
    server.dirty++;
    addReply(c, shared.ok);
}

int rdbSaveGtidInfoAuxFields(rio* rdb) {
    size_t maxlen = gtidSetEstimatedEncodeBufferSize(server.gtid_executed);
    char *gtid_str = zmalloc(maxlen);
    size_t gtid_str_len = gtidSetEncode(gtid_str, maxlen, server.gtid_executed);
    if (rdbSaveAuxField(rdb, "gtid", 4, gtid_str, gtid_str_len) == -1) {
        zfree(gtid_str);
        return -1;
    }
    zfree(gtid_str);
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

/* ctrip.merge_start {gid [crdt]} */
void ctripMergeStartCommand(client* c) {
    //not support crdt gid
    // server.gtid_in_merge = 1;
    c->gtid_in_merge = 1;
    addReply(c, shared.ok);
    server.dirty++;
}

/* ctrip.merge_set gid 1 version 1.0 */
void ctripMergeSetCommand(client* c) {
    //will set gid bind to client
    UNUSED(c);
}

/* merge key(string) value(robj) expire(long long) lfu_freq lru_idle */
void ctripMergeCommand(client* c) {
    if(c->gtid_in_merge == 0) {
        addReplyErrorFormat(c, "full sync failed");
        return;
    }
    //not support crdt gid

    robj *key = c->argv[1];
    rio payload;
    robj *val = NULL;
    // check val
    if(verifyDumpPayload(c->argv[2]->ptr, sdslen(c->argv[2]->ptr)) == C_ERR) {
        addReplyErrorFormat(c, "value robj load error: %s", (char*)c->argv[2]->ptr);
        goto error;
    }
    int type = -1;
    long long expiretime = -1, now = mstime();
    //check expiretime
    if(!string2ll((sds)c->argv[3]->ptr, sdslen((sds)c->argv[3]->ptr), &expiretime)) {
        addReplyErrorFormat(c, "expiretime string2ll error: %s", (char*)c->argv[3]->ptr);
        goto error;
    }
    //check lfu_freq lru_idle
    long long lfu_freq = -1, lru_idle = -1;
    if(c->argc == 6) {
        if(!string2ll((sds)c->argv[4]->ptr, sdslen(c->argv[4]->ptr), &lfu_freq)) {
            addReplyErrorFormat(c, "lfu_freq string2ll error: %s", (char*)c->argv[4]->ptr);
            goto error;
        }
        if(!string2ll((sds)c->argv[5]->ptr, sdslen(c->argv[5]->ptr), &lru_idle)) {
            addReplyErrorFormat(c, "lru_idle string2ll error: %s", (char*)c->argv[5]->ptr);
            goto error;
        }
    }

    rioInitWithBuffer(&payload, c->argv[2]->ptr);
    int load_error = 0;
    if (((type = rdbLoadObjectType(&payload)) == -1) ||
        ((val = rdbLoadObject(type, &payload, payload.io.buffer.ptr, &load_error)) == NULL))
    {
        addReplyErrorFormat(c, "load robj error: %d, key: %s", load_error, (char*)c->argv[2]->ptr);
        sdsfree(payload.io.buffer.ptr);
        goto error;
    }

    /* Check if the key already expired. This function is used when loading
        * an RDB file from disk, either at startup, or when an RDB was
        * received from the master. In the latter case, the master is
        * responsible for key expiry. If we would expire keys here, the
        * snapshot taken by the master may not be reflected on the slave.
        * Similarly if the RDB is the preamble of an AOF file, we want to
        * load all the keys as they are, since the log of operations later
        * assume to work in an exact keyspace state. */
    if (iAmMaster() &&
        expiretime != -1 && expiretime < now)
    {
        decrRefCount(val);
    } else {
        /* Add the new object in the hash table */
        sds keydup = sdsdup(key->ptr); /* moved to db.dict by dbAddRDBLoad */

        int added = dbAddRDBLoad(c->db,keydup,val);
        if (!added) {
            /**
             * When it's set we allow new keys to replace the current
                    keys with the same name.
             */
            dbSyncDelete(c->db,key);
            dbAddRDBLoad(c->db,keydup,val);
        }

        /* Set the expire time if needed */
        if (expiretime != -1) {
            setExpire(NULL,c->db,key,expiretime);
        }

        /* Set usage information (for eviction). */
        long long lru_clock = LRU_CLOCK();
        if(c->argc== 6) {
            objectSetLRUOrLFU(val,lfu_freq,lru_idle,lru_clock,1000);
        }
        /* call key space notification on key loaded for modules only */
        moduleNotifyKeyspaceEvent(NOTIFY_LOADED, "loaded", key, c->db->id);
    }

    /* Loading the database more slowly is useful in order to test
     * certain edge cases. */
    if (server.key_load_delay) usleep(server.key_load_delay);
    server.dirty++;
    addReply(c, shared.ok);
    return;
error:
    if(val != NULL) {
        decrRefCount(val);
    }
    c->gtid_in_merge = 0;
}

/* ctrip.merge_end {gtid_set} {gid} */
void ctripMergeEndCommand(client* c) {
    if(c->gtid_in_merge == 0) {
        addReplyErrorFormat(c, "full sync failed");
        return;
    }
    c->gtid_in_merge = 0;
    gtidSet* gtid_set = gtidSetDecode(c->argv[1]->ptr, sdslen(c->argv[1]->ptr));
    gtidSetMerge(server.gtid_executed, gtid_set);
    server.dirty++;
    addReply(c, shared.ok);
}

void gtidGetRobjCommand(client* c) {
    robj* key = c->argv[1];
    rio payload;
    robj* val;
    if ((val = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp])) == NULL)
        return;

    if (val) {
        createDumpPayload(&payload, val, key);
        addReplyBulkCBuffer(c, payload.io.buffer.ptr, sdslen(payload.io.buffer.ptr));
        sdsfree(payload.io.buffer.ptr);
    } else {
        addReplyNull(c);
    }

}

/* xpipe use info gtid to collect gtid gap string, but gap can be large
   and info command are often use to collect metric, encode gap into info
   might cause latency. To keep compatible without affecting metric
collect: info gtid expose complete gap info, info all & info expose
gap summary. */
sds genGtidGapString(sds info) {
    size_t maxlen = gtidSetEstimatedEncodeBufferSize(server.gtid_executed);
    char *buf = zcalloc(maxlen);
    size_t len = gtidSetEncode(buf, maxlen, server.gtid_executed);
    info = sdscatprintf(info, "all:%.*s\r\n", (int)len, buf);
    zfree(buf);
    return info;
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
            "    Return uuid.",
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
    } else {
        addReplySubcommandSyntaxError(c);
    }

}
