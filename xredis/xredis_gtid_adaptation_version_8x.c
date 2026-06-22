#include "server.h"
#include "xredis_gtid.h"
#include "xredis_gtid_cmdparse.h"
#include "xredis_gtid_adaptation_version.h"
/* version 8.x*/

/* dict */
dict* gtidDictCreate(dictType *type) {
    return dictCreate(type);
}

struct redisCommand* gtidLookupCommandBySds(sds name) {
    return lookupCommandBySds(name);
}

char* gtidGetCmdName(struct redisCommand* cmd) {
    return cmd->fullname;
}

/* obj  */
int gitdCmdGetKeyType(struct redisCommand *cmd) {
    if (cmd == NULL) return OBJ_UNKNOWN;
    if (cmd->group == COMMAND_GROUP_STRING) return OBJ_STRING;
    if (cmd->group == COMMAND_GROUP_LIST) return OBJ_LIST;
    if (cmd->group == COMMAND_GROUP_HASH) return OBJ_HASH;
    if (cmd->group == COMMAND_GROUP_SET) return OBJ_SET;
    if (cmd->group == COMMAND_GROUP_SORTED_SET) return OBJ_ZSET;
    if (cmd->group == COMMAND_GROUP_BITMAP) return OBJ_STRING;

    return OBJ_UNKNOWN;
}

char* gtidRedisCommandGetName(struct redisCommand* cmd) {
    return cmd->fullname;
}

char* gtidGetTypeName(int key_type) {
    if (key_type == OBJ_UNKNOWN) {
        return "unknow";
    }
    robj o = {.type = key_type};
    return getObjectTypeName(&o);
}

long long gtidGetBacklogOffset() {
    if (!server.repl_backlog) return 0;
    return server.repl_backlog->offset;
}

long long gtidGetBacklogHistlen() {
    if (!server.repl_backlog) return 0;
    return server.repl_backlog-> histlen;
}

/* getKeysResult*/
int gtidGetKeysResultKeyIndex(getKeysResult* result, int index) {
    return result->keys[index].pos;
}

/* can't get old value */
void ctrip_resizeReplicationBacklog(long long newsize) {
    UNUSED(newsize);
    resizeReplicationBacklog();
    if (server.repl_backlog != NULL) {
        /* realloc a new gtidSeq to keep gtid_seq sync with backlog, see
         * resizeReplicationBacklog for more details. */
        gtidSeqDestroy(server.gtid_seq);
        server.gtid_seq = gtidSeqCreate();
    }
}

void ctrip_replicationFeedSlaves(list* saves,int dictid, robj **argv,
        int argc, const char *uuid, size_t uuid_len, gno_t gno, long long offset) {
    int touch_index = uuid != NULL && gno >= GTID_GNO_INITIAL && server.gtid_seq
        && server.masterhost == NULL;
#ifdef ENABLE_SWAP
    touch_index = touch_index && server.swap_draining_master == NULL;
#endif
   
    if (touch_index) gtidSeqAppend(server.gtid_seq,uuid,uuid_len,gno,offset);
    replicationFeedSlaves(saves, dictid,argv,argc);
}

void ctrip_replicationFeedSlavesFromMasterStream(list *slaves, char *buf,
        size_t buflen, const char *uuid, size_t uuid_len, gno_t gno, long long offset) {
    UNUSED(slaves);
    int touch_index = uuid != NULL && gno >= GTID_GNO_INITIAL && server.gtid_seq;
    if (touch_index) gtidSeqAppend(server.gtid_seq,uuid,uuid_len,gno,offset);
    replicationFeedStreamFromMasterStream(buf,buflen);
}

long long consumeReplicationBacklogLimited(long long offset, long long limit,
        consume_cb cb, void *pd) {
    long long skip;
    long long added = 0;
    if (server.repl_backlog->histlen == 0) {
        serverLog(LL_DEBUG, "[PSYNC] Backlog history len is zero");
        return 0;
    }
    /* Compute the amount of bytes we need to discard. */
    skip = offset - server.repl_backlog->offset;
    long long len = server.repl_backlog->histlen - skip;
    len = len < limit ? len: limit;
    /* Iterate recorded blocks, quickly search the approximate node. */
    listNode *node = NULL;
    if (raxSize(server.repl_backlog->blocks_index) > 0) {
        uint64_t encoded_offset = htonu64(offset);
        raxIterator ri;
        raxStart(&ri, server.repl_backlog->blocks_index);
        raxSeek(&ri, ">", (unsigned char*)&encoded_offset, sizeof(uint64_t));
        if (raxEOF(&ri)) {
            /* No found, so search from the last recorded node. */
            raxSeek(&ri, "$", NULL, 0);
            raxPrev(&ri);
            node = (listNode *)ri.data;
        } else {
            raxPrev(&ri); /* Skip the sought node. */
            /* We should search from the prev node since the offset of current
             * sought node exceeds searching offset. */
            if (raxPrev(&ri))
                node = (listNode *)ri.data;
            else
                node = server.repl_backlog->ref_repl_buf_node;
        }
        raxStop(&ri);
    } else {
        /* No recorded blocks, just from the start node to search. */
        node = server.repl_backlog->ref_repl_buf_node;
    }

    /* Search the exact node. */
    while (node != NULL) {
        replBufBlock *o = listNodeValue(node);
        if (o->repl_offset + (long long)o->used >= offset) break;
        node = listNextNode(node);
    }
    serverAssert(node != NULL);

    serverLog(LL_WARNING, "start copy backlog offset(%lld) len(%lld)", offset, len);
    while (len) {
        replBufBlock *o = listNodeValue(node);
        int start = 0;
        if (offset > o->repl_offset) {
            start = offset - o->repl_offset;
        }
        long long thislen = o->used - start  < len?
                o->used - start: len;
        serverLog(LL_WARNING, "block start(%lld), offset(%lld) size(%lld)", o->repl_offset, start, thislen);
        cb(o->buf + start, thislen, pd);
        len -= thislen;
        start = 0;
        added += thislen;
        node = listNextNode(node);
    }
    serverLog(LL_WARNING, "added=%lld", added);
    return added;
}

void consumeReplicationBacklogLimitedAddReplyCb(char *p,
        long long thislen, void *pd) {
    //can't use addReplySds send slave message, because client will be closed by underlying layer  
    if (connWrite(((client*)pd)->conn,p,thislen) != thislen) {
        serverLog(LL_WARNING, "[consumeReplicationBacklogLimitedAddReplyCb] send slave fail");
        freeClientAsync((client*)pd);
    }
}

void gtidClearReplStartCmdStreamOnAck(client* c) {
    c->repl_start_cmd_stream_on_ack = 0;
}

void gtidFreeClientAsync(client* c) {
    serverLog(LL_WARNING, "add CLIENT_CLOSE_AFTER_REPLY is CLIENT_CLOSE_ASAP %d", c->flags & CLIENT_CLOSE_ASAP);
    freeClientAsync(c);
    // c->flags |= CLIENT_CLOSE_AFTER_REPLY;
}

int gtidMasterTryPartialResynchronization(client* c, long long psync_offset) {
    return masterTryPartialResynchronization(c, psync_offset);
}

int isExecCommand(struct redisCommand *cmd) {
    return cmd->proc == execCommand;
}

int isGtidCommand(struct redisCommand *cmd) {
    return cmd->proc == gtidCommand;
}

void gtidAfterErrorReply(client *c, const char *s, size_t len, int flags) {
    afterErrorReply(c, s, len, flags);
}

void feedAppendOnlyFileGtid(struct redisCommand* cmd, int dictid, robj **argv, int argc) {
    sds buf = sdsempty();

    if(cmd == NULL) cmd = lookupCommandByCString(argv[GTID_COMMAN_ARGC]->ptr);
    serverAssert(cmd != NULL);

    if (dictid != -1 && dictid != server.aof_selected_db) {
        char seldb[64];

        snprintf(seldb,sizeof(seldb),"%d",dictid);
        buf = sdscatprintf(buf,"*2\r\n$6\r\nSELECT\r\n$%lu\r\n%s\r\n",
            (unsigned long)strlen(seldb),seldb);
        server.aof_selected_db = dictid;
    }

    /* All commands should be propagated the same way in AOF as in replication.
     * No need for AOF-specific translation. */
    buf = catAppendOnlyGenericCommand(buf,argc,argv);

    if (server.aof_state == AOF_ON ||
        (server.aof_state == AOF_WAIT_REWRITE && server.child_type == CHILD_TYPE_AOF))
        server.aof_buf = sdscatlen(server.aof_buf,buf,sdslen(buf));

    sdsfree(buf);
}

void ctrip_feedAppendOnlyFile(struct redisCommand *cmd, int dictid,
        robj **argv, int argc) {
    if (isGtidCommand(cmd)) 
        feedAppendOnlyFileGtid(cmd,dictid,argv,argc);
    else
        feedAppendOnlyFile(dictid,argv,argc);
}

struct redisCommand* gtidGetGtidCommand() {
    static struct redisCommand* gtid = NULL;
    if (gtid == NULL) {
        gtid = lookupCommandByCString("gtid");
        serverAssert(gtid != NULL);
    }
    return gtid;
}
struct redisCommand* gtidGetExecCommand() {
    static struct redisCommand* exec = NULL;
    if (exec == NULL) {
        exec = lookupCommandByCString("exec");
        serverAssert(exec != NULL);
    }
    return exec;
}

/* client */
void gtidMockClientInit(client* c) {
    c->querybuf = sdsempty();
    c->authenticated = 1;
    c->argv = NULL;
    c->argc = 0;
    c->qb_pos = 0;
    c->flags = 0;
    c->bulklen =- 1;
    c->multibulklen = 0;
    c->argv_len = 0;
}
void gtidMockClientDeinit(client* c) {
    gtidMockClientCleanArgv(c);
    zfree(c->argv);
    c->argv_len = 0;
    sdsfree(c->querybuf);
    c->querybuf = NULL;
    c->qb_pos = 0;
}
void gtidMockClientCleanArgv(client* c) {
    if (c->argv) {
        for (int i = 0; i < c->argc; i++)
            if (c->argv[i]) decrRefCount(c->argv[i]);
        c->argc = 0;
    }
}

void gtidMockClientMoveClientArgv(client* c) {
    c->argc = 0;
    c->argv = NULL;
    c->argv_len = 0;
}


long long gtidBacklogAppendToSds(long long offset, sds *dst, size_t size) {
    if (server.repl_backlog == NULL || server.repl_backlog->histlen == 0)
        return -1;

    long long skip = offset - server.repl_backlog->offset;
    if (skip < 0 || skip >= server.repl_backlog->histlen) return -1;

    long long available = server.repl_backlog->histlen - skip;
    if (available <= 0) return -1;
    if ((long long)size > available) size = (size_t)available;

    listNode *node = NULL;
    if (raxSize(server.repl_backlog->blocks_index) > 0) {
        uint64_t encoded_offset = htonu64(offset);
        raxIterator ri;
        raxStart(&ri, server.repl_backlog->blocks_index);
        raxSeek(&ri, ">", (unsigned char*)&encoded_offset, sizeof(uint64_t));
        if (raxEOF(&ri)) {
            raxSeek(&ri, "$", NULL, 0);
            raxPrev(&ri);
            node = (listNode *)ri.data;
        } else {
            raxPrev(&ri);
            if (raxPrev(&ri))
                node = (listNode *)ri.data;
            else
                node = server.repl_backlog->ref_repl_buf_node;
        }
        raxStop(&ri);
    } else {
        node = server.repl_backlog->ref_repl_buf_node;
    }

    while (node != NULL) {
        replBufBlock *o = listNodeValue(node);
        if (o->repl_offset + (long long)o->used > offset) break;
        node = listNextNode(node);
    }
    if (node == NULL) return -1;

    *dst = sdsMakeRoomFor(*dst, size);

    size_t total = 0;
    while (total < size && node != NULL) {
        replBufBlock *o = listNodeValue(node);
        size_t block_skip = (total == 0) ? (offset - o->repl_offset) : 0;
        size_t thislen = o->used - block_skip;
        if (thislen > size - total) thislen = size - total;

        memcpy((*dst) + sdslen(*dst) + total, o->buf + block_skip, thislen);
        total += thislen;
        node = listNextNode(node);
    }
    sdsIncrLen(*dst, total);

    return total;
}

/* test */
void gtidInitTestEnv() {
    server.repl_buffer_blocks = listCreate();
    server.slaves = listCreate();
}
void gtidFeedReplicationBacklog(void* buffer, size_t len) {
    feedReplicationBuffer(buffer, len);
}