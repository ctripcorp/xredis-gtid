#include "server.h"
#include "xredis_gtid_adaptation_version.h"
#include <gtid.h>
#include <ctype.h>

void gtidGaplogNodeInit(gtidGaplogNode* node, robj* uuid, gno_t gno, gtidGaplogKeys* keys) {
    node->uuid = uuid;
    node->gno = gno;
    node->keys = keys;
}

void gtidGaplogNodeDeinit(gtidGaplogNode* node) {
    if (node == NULL) return;
    if (node->uuid) {
        decrRefCount(node->uuid);
        node->uuid = NULL;
        gtidGaplogKeysRelease(node->keys);
        node->keys = NULL;
    }

}

gtidGaplog* gtidGaplogNew(size_t capacity) {
    gtidGaplog* gaplog =  zmalloc(sizeof(gtidGaplog));
    gaplog->data = zmalloc(sizeof(gtidGaplogNode) * capacity);
    for(size_t i = 0; i < capacity; i++) {
        gtidGaplogNodeInit(&gaplog->data[i], NULL, 0, NULL);
    }
    gaplog->len = 0;
    gaplog->index = 0;
    gaplog->all = gtidSetNew();
    gaplog->capacity = capacity;
    return gaplog;
}

void gtidGaplogRelease(gtidGaplog* gaplog) {
    gtidGaplogDataIterator iter;
    gtidGaplogInitDataIterator(&iter, gaplog, 0);
    gtidGaplogNode* node = NULL;
    while((node = gtidGaplogDataNext(&iter))) {
        gtidGaplogNodeDeinit(node);
    }
    gtidGaplogDeinitDataIterator(&iter);
    zfree(gaplog->data);
    gaplog->len = 0;
    gaplog->index = 0;
    gtidSetFree(gaplog->all);
    zfree(gaplog);
}

void gtidGaplogResetDataSize(gtidGaplog* gaplog, size_t new_size) {
    if (gaplog->capacity == new_size) {
        return;
    }
    gtidGaplogNode* nodes = zmalloc(sizeof(gtidGaplogNode) * new_size);
    size_t start_index = 0;
    if (gaplog->len > new_size) {
        /* contraction */
        start_index = gaplog->len - new_size;
    } else {
        /* Expansion */
        start_index = 0;
    }

    gtidGaplogDataIterator iter;
    gtidGaplogInitDataIterator(&iter, gaplog, 0);
    gtidGaplogNode* node = NULL;
    size_t i = 0;
    size_t ni = 0;
    while((node = gtidGaplogDataNext(&iter))) {
        if (i >= start_index) {
            nodes[ni].uuid = node->uuid;
            node->uuid = NULL; /* move */
            nodes[ni].gno = node->gno;
            nodes[ni].keys = node->keys;
            node->keys = NULL; /* move */
            ni++;
        } else {
            gtidSetRemove(gaplog->all, node->uuid->ptr, sdslen(node->uuid->ptr), node->gno, node->gno);
            decrRefCount(node->uuid);
            gtidGaplogKeysRelease(node->keys);
        }
        i++;
    }
    gtidGaplogDeinitDataIterator(&iter);
    zfree(gaplog->data);
    gaplog->data = nodes;
    gaplog->len = ni;
    gaplog->index = 0;
    gaplog->capacity = new_size;
}

void gtidGaplogKeysRelease(void *data) {
    if (data == NULL) return;
    gtidGaplogKeys* keys = (gtidGaplogKeys*)data;  
    for (size_t i = 0; i < keys->size; i++) {
        gtidGaplogKeyRelease(keys->keys[i]);
    }
    zfree(keys->keys);
    zfree(keys);
}

/*gap log key info*/
gtidGaplogKey* gtidGaplogKeyNew(int dbid, int type, sds key, sds* subkeys, int subkeys_count) {
    gtidGaplogKey *ki = zcalloc(sizeof(gtidGaplogKey));
    ki->dbid = dbid;
    ki->key_type = type;
    ki->key = key;           /* move */
    ki->subkeys = subkeys;   /* move */
    ki->subkeys_count = subkeys_count;
    return ki;
}

void gtidGaplogKeyRelease(gtidGaplogKey* ki) {
    if (ki == NULL) return;
    sdsfree(ki->key);
    for (size_t i = 0; i < ki->subkeys_count; i++) {
        sdsfree(ki->subkeys[i]);
    }
    zfree(ki->subkeys);
    zfree(ki);
}

gtidGaplogKey** gtidGaplogKeysPrepareBuilder(gtidGaplogKeysBuilder* builder, int add_numkeys) {
    if (!builder->keys_infos) {
        builder->keys_infos = builder->cache;
    }

    if (add_numkeys  + builder->numkeys > builder->size) {
        long long update_numkeys = builder->size;
        while(update_numkeys < (add_numkeys + builder->numkeys)) {
          update_numkeys *= 2;
        }
        if (builder->keys_infos != builder->cache) {
            builder->keys_infos = zrealloc(builder->keys_infos, sizeof(gtidGaplogKey*) * ( update_numkeys));
        } else {
            builder->keys_infos = zmalloc(sizeof(gtidGaplogKey*) * (update_numkeys));
            if (builder->numkeys)
                memcpy(builder->keys_infos, builder->cache, sizeof(gtidGaplogKey*) * builder->numkeys);
        }
        builder->size = update_numkeys;
    }
    return builder->keys_infos + builder->numkeys;
}

void gtidGaplogDeinitKeysBuilder(gtidGaplogKeysBuilder* builer) {
    for (int i  = 0; i < builer->numkeys; i++) {
        gtidGaplogKeyRelease(builer->keys_infos[i]);  
        builer->keys_infos[i] = NULL;
    }
    if (builer && builer->keys_infos != builer->cache) {
        zfree(builer->keys_infos);
    }   
}

gtidGaplogKeys* gtidGaplogKeysBuild(gtidGaplogKeysBuilder* builder) {
    gtidGaplogKeys* keys = zmalloc(sizeof(gtidGaplogKeys));
    keys->size = builder->numkeys;
    /*move keys*/
    keys->keys =zmalloc(sizeof(gtidGaplogKey*) * keys->size);
    for(int i = 0; i < builder->numkeys; i++) {
        keys->keys[i] = builder->keys_infos[i];
        builder->keys_infos[i] = NULL;
    }
    builder->numkeys = 0;
    return keys;
}

/* ========== gtidGaplog History iterator ========== */
void gtidGaplogInitDataIterator(gtidGaplogDataIterator* iter,
                                    gtidGaplog* gaplog, size_t index) {
    iter->gaplog = gaplog;
    iter->index = index;
}

gtidGaplogNode* gtidGaplogDataNext(gtidGaplogDataIterator* iter) {
    if (iter->index >= iter->gaplog->len) {
        return NULL;
    }
    size_t pos = iter->gaplog->index + iter->index;
    if (pos >= iter->gaplog->capacity) pos -= iter->gaplog->capacity;
    gtidGaplogNode* node = &iter->gaplog->data[pos];
    iter->index++;
    return node;
}

void gtidGaplogDeinitDataIterator(gtidGaplogDataIterator* iter) {
    UNUSED(iter);
}

void gtidGaplogDataIteratorSeek(gtidGaplogDataIterator* iter, size_t index) {
    if (index > iter->gaplog->len) {
        iter->index = iter->gaplog->len;
        return;
    }
    iter->index = index;
}

void addReplyGtidGaplogKeys(client* c, gtidGaplogKeys* keys) {
    addReplyArrayLen(c, keys->size);
    for (size_t i = 0; i < keys->size; i++) {
        gtidGaplogKey *k = keys->keys[i];
        addReplyArrayLen(c, 4);
        addReplyBulkLongLong(c, k->dbid);
        addReplyBulkCBuffer(c, k->key, sdslen(k->key));
        addReplyBulkCString(c, gtidGetTypeName(k->key_type));
        addReplyArrayLen(c, k->subkeys_count);
        for (size_t j = 0; j < k->subkeys_count; j++) {
            addReplyBulkCBuffer(c, k->subkeys[j], sdslen(k->subkeys[j]));
        }
    }
}

int gtidGaplogInsert(gtidGaplog* gaplog, robj* uuid, gno_t gno, gtidGaplogKeys* keys) {
    gtidGaplogNode* last_node = NULL;
    if (gaplog->len == gaplog->capacity) {
        last_node = &gaplog->data[gaplog->index];
        gtidSetRemove(gaplog->all,last_node->uuid->ptr, sdslen(last_node->uuid->ptr), last_node->gno, last_node->gno);
        decrRefCount(last_node->uuid);
        gtidGaplogKeysRelease(last_node->keys);
        gaplog->index = (gaplog->index + 1) % gaplog->capacity;
    } else {
        last_node = &gaplog->data[(gaplog->index + gaplog->len)%gaplog->capacity];
        gaplog->len++;
    }
    serverAssert(last_node != NULL);
    incrRefCount(uuid);
    last_node->uuid = uuid;
    last_node->gno = gno;
    last_node->keys = keys;
    gtidSetAdd(gaplog->all, uuid->ptr, sdslen(uuid->ptr), gno, gno);
    return 1;
}

size_t gtidGaplogSize(gtidGaplog* gaplog) {
    if (gaplog == NULL) return 0;
    return gaplog->len;
}

gtidSet* gtidGaplogGetAll(gtidGaplog* gaplog) {
    if (gaplog == NULL) return NULL;
    return gaplog->all;
}

void gtidGaplogKeysBuilderAdd(gtidGaplogKeysBuilder *builder, int dbid, int type, sds key,
                       sds *subkeys, int subkeys_count)
{
    gtidGaplogKeysPrepareBuilder(builder, 1);
    gtidGaplogKey *key_result = gtidGaplogKeyNew(dbid, type, key, subkeys, subkeys_count);
    builder->keys_infos[builder->numkeys++] = key_result;
}

static void gtidOnKey(void *ctx, int dbid, struct redisCommand* cmd, robj** argv, int argc,  int key_arg_idx,
                      int subkeys_count, int subkeys_start,
                      int subkeys_step, const int *subkey_arg_idxs,
                      const cmdParseKeyExtra *extra)
{
    UNUSED(extra);
    UNUSED(argc);
    gtidGaplogKeysBuilder *builder = ctx;
    sds key = sdsdup((sds)argv[key_arg_idx]->ptr);
    sds *subkeys = subkeys_count > 0 ? zmalloc(sizeof(sds) * subkeys_count) : NULL;
    for (int i = 0; i < subkeys_count; i++) {
        int subkey_idx = subkey_arg_idxs ? subkey_arg_idxs[i] : (subkeys_start + i * subkeys_step);
        subkeys[i] = sdsdup((sds)argv[subkey_idx]->ptr);
    }
    gtidGaplogKeysBuilderAdd(builder, dbid, gitdCmdGetKeyType(cmd), key, subkeys, subkeys_count);
}

void gtidGaplogKeysBuilderAddFromCmd(gtidGaplogKeysBuilder *builder, int dbid, robj **args, int argc) {
    if (argc < 2) return;
    serverAssert( builder != NULL);
    cmdParseKeys(dbid, NULL, args, argc, builder, gtidOnKey);
}

int gtidGaplogList(gtidGaplog* gaplog, long long start_idx, long long count,
                  gtidGaplogListCallbackFn callback,
                   void* ctx) {
    gtidGaplogDataIterator hist_iter;
    gtidGaplogInitDataIterator(&hist_iter, gaplog, start_idx);

    int nreply = 0;

    while (nreply < count) {
        gtidGaplogNode *node = gtidGaplogDataNext(&hist_iter);
        if (node == NULL || node->uuid == NULL) break;

        callback(node->uuid, node->gno, node->keys, ctx);
        nreply++;
    }
    gtidGaplogDeinitDataIterator(&hist_iter);
    return nreply;
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
    it->mock.bulklen = -1;  /* processMultibulkBuffer */
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
    /* offset < cur (rewind) or offset > end: clear+seek */
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



void gtidGaplogFillFromGtidSet(gtidSet *mlost) {
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
            robj* shard_uuid = createStringObject(us->uuid, us->uuid_len);
            sds uuid = shard_uuid->ptr;
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
                    serverLog(LL_WARNING, "[gaplog] gtidGaplogFillFromGtidSet unexpected command %s", cmd_name);
                }

                if (builder.numkeys > 0) {
                    gtidGaplogInsert(server.gtid_gap_log, shard_uuid, gno, gtidGaplogKeysBuild(&builder));
                }
                gtidGaplogDeinitKeysBuilder(&builder);

            }
            decrRefCount(shard_uuid);
        }
        uuidSetDeinitIterator(&us_iterator);
    }
    gtidSetDeinitIterator(&gs_iterator);

    readBacklogIteratorDeinit(&it);
}

#ifdef REDIS_TEST

int readBacklogIteratorTest(int argc, char **argv, int accurate) {
    UNUSED(argc), UNUSED(argv), UNUSED(accurate);
    int error = 0;

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

        /* no-op seek: offset == cur */
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
        gtidFeedReplicationBacklog(cmd, sdslen(cmd));
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

int gapLogTest(int argc, char **argv, int accurate) {
    UNUSED(argc), UNUSED(argv), UNUSED(accurate);
    int error = 0;

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

    TEST("gtid - gapLog empty lifecycle") {
        gtidGaplog *gaplog = gtidGaplogNew(3);
        test_assert(gtidGaplogSize(gaplog) == 0);

        gtidGaplogDataIterator iter;
        gtidGaplogInitDataIterator(&iter, gaplog, 0);
        test_assert(gtidGaplogDataNext(&iter) == NULL);
        gtidGaplogDeinitDataIterator(&iter);
        gtidGaplogRelease(gaplog);
    }

    TEST("gtid - gapLog preserves insertion order before wrap") {
        gtidGaplog *gaplog = gtidGaplogNew(3);
        robj *uuid1 = createStringObject("uuid-1", 6);
        robj *uuid2 = createStringObject("uuid-2", 6);
        gtidGaplogKeysBuilder builder1 = GTID_GAPLOG_KEYS_BUILDER_INIT;
        gtidGaplogKeysBuilder builder2 = GTID_GAPLOG_KEYS_BUILDER_INIT;
        gtidGaplogInsert(gaplog, uuid1, 11, gtidGaplogKeysBuild(&builder1));
        gtidGaplogInsert(gaplog, uuid2, 22, gtidGaplogKeysBuild(&builder2));
        decrRefCount(uuid1);
        decrRefCount(uuid2);

        test_assert(gtidGaplogSize(gaplog) == 2);
        gtidGaplogDataIterator iter;
        gtidGaplogInitDataIterator(&iter, gaplog, 0);
        gtidGaplogNode *node = gtidGaplogDataNext(&iter);
        test_assert(node != NULL && node->gno == 11 && !strcmp(node->uuid->ptr, "uuid-1"));
        node = gtidGaplogDataNext(&iter);
        test_assert(node != NULL && node->gno == 22 && !strcmp(node->uuid->ptr, "uuid-2"));
        test_assert(gtidGaplogDataNext(&iter) == NULL);
        gtidGaplogDeinitDataIterator(&iter);
        gtidGaplogRelease(gaplog);
    }

    TEST("gtid - gapLog overwrites oldest entry after wrap") {
        gtidGaplog *gaplog = gtidGaplogNew(3);
        for (gno_t gno = 1; gno <= 4; gno++) {
            char uuid_buf[32];
            int uuid_len = snprintf(uuid_buf, sizeof(uuid_buf), "%lld", (long long)gno);
            robj *uuid = createStringObject(uuid_buf, uuid_len);
            gtidGaplogKeysBuilder builder = GTID_GAPLOG_KEYS_BUILDER_INIT;
            gtidGaplogInsert(gaplog, uuid, gno, gtidGaplogKeysBuild(&builder));
            decrRefCount(uuid);
        }

        test_assert(gtidGaplogSize(gaplog) == 3);
        gtidGaplogDataIterator iter;
        gtidGaplogInitDataIterator(&iter, gaplog, 0);
        for (gno_t expected = 2; expected <= 4; expected++) {
            gtidGaplogNode *node = gtidGaplogDataNext(&iter);
            long long uuid_gno;
            test_assert(node != NULL && node->gno == expected);
            test_assert(getLongLongFromObject(node->uuid, &uuid_gno) == C_OK && uuid_gno == expected);
        }
        test_assert(gtidGaplogDataNext(&iter) == NULL);
        gtidGaplogDeinitDataIterator(&iter);
        gtidGaplogRelease(gaplog);
    }

    TEST("gtid - gapLog iterator seek honors end boundary") {
        gtidGaplog *gaplog = gtidGaplogNew(3);
        for (gno_t gno = 1; gno <= 3; gno++) {
            robj *uuid = createStringObject("uuid", 4);
            gtidGaplogKeysBuilder builder = GTID_GAPLOG_KEYS_BUILDER_INIT;
            gtidGaplogInsert(gaplog, uuid, gno, gtidGaplogKeysBuild(&builder));
            decrRefCount(uuid);
        }

        gtidGaplogDataIterator iter;
        gtidGaplogInitDataIterator(&iter, gaplog, 0);
        gtidGaplogDataIteratorSeek(&iter, 1);
        gtidGaplogNode *node = gtidGaplogDataNext(&iter);
        test_assert(node != NULL && node->gno == 2);
        gtidGaplogDataIteratorSeek(&iter, gtidGaplogSize(gaplog));
        test_assert(gtidGaplogDataNext(&iter) == NULL);
        gtidGaplogDeinitDataIterator(&iter);
        gtidGaplogRelease(gaplog);
    }

    TEST("gtid - gapLog GetAll returns correct gtid set") {
        gtidGaplog *gaplog = gtidGaplogNew(10);

        gtidSet *all = gtidGaplogGetAll(gaplog);
        test_assert(gtidSetCount(all) == 0);

        robj *uid1 = createStringObject("uid-a", 5);
        robj *uid2 = createStringObject("uid-b", 5);
        gtidGaplogKeysBuilder b1 = GTID_GAPLOG_KEYS_BUILDER_INIT;
        gtidGaplogInsert(gaplog, uid1, 1, gtidGaplogKeysBuild(&b1));
        gtidGaplogKeysBuilder b2 = GTID_GAPLOG_KEYS_BUILDER_INIT;
        gtidGaplogInsert(gaplog, uid1, 2, gtidGaplogKeysBuild(&b2));
        gtidGaplogKeysBuilder b3 = GTID_GAPLOG_KEYS_BUILDER_INIT;
        gtidGaplogInsert(gaplog, uid2, 10, gtidGaplogKeysBuild(&b3));

        all = gtidGaplogGetAll(gaplog);
        test_assert(gtidSetCount(all) == 3);
        test_assert(gtidSetContains(all, "uid-a", 5, 1));
        test_assert(gtidSetContains(all, "uid-a", 5, 2));
        test_assert(gtidSetContains(all, "uid-b", 5, 10));

        decrRefCount(uid1);
        decrRefCount(uid2);
        gtidGaplogRelease(gaplog);
    }

    return error;
}
#endif
