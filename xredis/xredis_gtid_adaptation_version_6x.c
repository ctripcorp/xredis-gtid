#include "server.h"
#include "xredis_gtid.h"
#include "xredis_gtid_cmdparse.h"
#include "xredis_gtid_adaptation_version.h"
/* version 6.x*/

long long gtidBacklogAppendToSds(long long offset, sds *dst, size_t size) {
    if (server.repl_backlog == NULL || server.repl_backlog_histlen == 0)
        return -1;

    long long skip = offset - server.repl_backlog_off;
    if (skip < 0 || skip >= server.repl_backlog_histlen) return -1;

    long long available = server.repl_backlog_histlen - skip;
    if (available <= 0) return -1;
    if ((long long)size > available) size = (size_t)available;

    long long j = (server.repl_backlog_idx +
                   (server.repl_backlog_size - server.repl_backlog_histlen)) %
                   server.repl_backlog_size;
    j = (j + skip) % server.repl_backlog_size;

    *dst = sdsMakeRoomFor(*dst, size);

    size_t total = 0;
    while (total < size) {
        size_t thislen = server.repl_backlog_size - j;
        if (thislen > size - total) thislen = size - total;
        memcpy((*dst) + sdslen(*dst) + total, server.repl_backlog + j, thislen);
        total += thislen;
        j = 0;
    }
    sdsIncrLen(*dst, total);

    return (long long)total;
}

int gitdCmdGetKeyType(struct redisCommand *cmd) {
    if (cmd == NULL) return OBJ_UNKNOWN;
    if (cmd->flags & CMD_CATEGORY_STRING) return OBJ_STRING;
    if (cmd->flags & CMD_CATEGORY_LIST) return OBJ_LIST;
    if (cmd->flags & CMD_CATEGORY_HASH) return OBJ_HASH;
    if (cmd->flags & CMD_CATEGORY_SET) return OBJ_SET;
    if (cmd->flags & CMD_CATEGORY_SORTEDSET) return OBJ_ZSET;
    if (cmd->flags & CMD_CATEGORY_BITMAP) return OBJ_STRING;
    return OBJ_UNKNOWN;
}

char* gtidRedisCommandGetName(struct redisCommand* cmd) {
    return cmd->name;
}
char* gtidGetTypeName(int key_type) {
    robj o = {.type = key_type};
    return getObjectTypeName(&o);
}

char* gtidGetCmdName(struct redisCommand* cmd) {
    return cmd->name;
}

void gtidMockClientInit(client* mock) {
    mock->querybuf = sdsempty();
    mock->authenticated = 1;
    mock->argv = NULL;
    mock->argc = 0;
    mock->qb_pos = 0;
    mock->flags = 0;
    mock->bulklen =- 1;
    mock->multibulklen = 0;
}

void gtidMockClientCleanArgv(client* mock) {
    if (mock->argv) {
        for (int i = 0; i < mock->argc; i++)
            if (mock->argv[i]) decrRefCount(mock->argv[i]);
        zfree(mock->argv);
        mock->argv = NULL;
        mock->argc = 0;
    }
}

void gtidMockClientDeinit(client* mock) {
    gtidMockClientCleanArgv(mock);
    sdsfree(mock->querybuf);
    mock->querybuf = NULL;
    mock->qb_pos = 0;
}

void gtidMockClientMoveClientArgv(client *c) {
    c->argc = 0;
    c->argv = NULL;
}

dict* gtidDictCreate(dictType *type) {
    return dictCreate(type, NULL);
}

int gtidGetKeysResultKeyIndex(getKeysResult* result, int index) {
    return result->keys[index];
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

void ctrip_replicationFeedSlavesFromMasterStream(list *slaves, char *buf,
        size_t buflen, const char *uuid, size_t uuid_len, gno_t gno, long long offset) {
    int touch_index = uuid != NULL && gno >= GTID_GNO_INITIAL && server.gtid_seq;
    if (touch_index) gtidSeqAppend(server.gtid_seq,uuid,uuid_len,gno,offset);
    replicationFeedSlavesFromMasterStream(slaves,buf,buflen);
    if (touch_index) gtidSeqTrim(server.gtid_seq,server.repl_backlog_off);
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

void gtidClearReplStartCmdStreamOnAck(client* c) {
    c->repl_put_online_on_ack = 0;
}

void gtidFreeClientAsync(client* c) {
    c->flags |= CLIENT_CLOSE_AFTER_REPLY;
}

void consumeReplicationBacklogLimitedAddReplyCb(char *p,
        long long thislen, void *pd) {
    addReplySds((client*)pd, sdsnewlen(p,thislen));
}

int gtidMasterTryPartialResynchronization(client* c, long long psync_offset) {
    UNUSED(psync_offset);
    return masterTryPartialResynchronization(c);
}

int isExecCommand(struct redisCommand *cmd) {
    return cmd == server.execCommand;
}

int isGtidCommand(struct redisCommand *cmd) {
    return cmd == server.gtidCommand;
}

void gtidAfterErrorReply(client *c, const char *s, size_t len, int flags) {
    UNUSED(flags);
    afterErrorReply(c, s, len);
}

struct redisCommand* gtidLookupCommandBySds(sds name) {
    return lookupCommand(name);
}

/* gtid expireat command append buffer */
static sds catAppendOnlyGtidExpireAtCommand(sds buf, robj* gtid, robj* dbid,
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

    serverAssert(isGtidCommand(_cmd));

    cmd = gtidLookupCommandBySds(argv[GTID_COMMAN_ARGC]->ptr);

    if (dictid >= 0 && dictid != server.aof_selected_db) {
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
    if (isGtidCommand(cmd))
        feedAppendOnlyFileGtid(cmd,dictid,argv,argc);
    else
        feedAppendOnlyFile(cmd,dictid,argv,argc);
}

long long gtidGetBacklogOffset() {
    return server.repl_backlog_off;
}

long long gtidGetBacklogHistlen() {
    return server.repl_backlog_histlen;
}

struct redisCommand* gtidGetGtidCommand() {
    return server.gtidCommand;
}
struct redisCommand* gtidGetExecCommand() {
    return server.execCommand;
}     



/* test */
void gtidInitTestEnv() {
    
}
void gtidFeedReplicationBacklog(void* buffer, size_t len) {
    feedReplicationBacklog(buffer, len);
}