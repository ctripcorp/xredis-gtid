#include "server.h"
#include "xredis_gtid_adaptation_version.h"
#include <gtid.h>
#include <ctype.h>
static void uuidSetFreeWrapper(void *ptr) {
    uuidSet *us = (uuidSet*)ptr;
    if (us) {
        uuidSetFree(us);
    }
}

void gtidGaplogSkiplistDestructor(void *privdata, void *val) {
    UNUSED(privdata);
    skiplist *sl = (skiplist*)val;
    if (sl) {
        skiplistFree(sl);
    }
}

static dictType gtidGaplogDictType = {
    .hashFunction = dictSdsHash,
    .keyCompare = dictSdsKeyCompare,
    .keyDestructor = dictSdsDestructor,
    .valDestructor = gtidGaplogSkiplistDestructor
};

gtidGaplog* gtidGaplogNew() {
    gtidGaplog* gaplog =  zmalloc(sizeof(gtidGaplog));
    gaplog->data = gtidDictCreate(&gtidGaplogDictType);
    gaplog->size = 0;
    gaplog->history = listCreate();
    listSetFreeMethod(gaplog->history, uuidSetFreeWrapper);
    return gaplog;
}

void gtidGaplogReset(gtidGaplog* gaplog) {
    dictEmpty(gaplog->data, NULL);
    gaplog->size = 0;
    listEmpty(gaplog->history);
}

void gtidGaplogRelease(gtidGaplog* gaplog) {
    dictRelease(gaplog->data);
    listRelease(gaplog->history);
    gaplog->size = 0;
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
        while(builder->size < (add_numkeys + builder->numkeys)) {
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

/* ========== gtidGaplog Data iterator ========== */
void gtidGaplogDataInitIterator(gtidGaplogDataIterator *iter, skiplist *sl, gno_t start_gno) {
    skiplistInitIterator(&iter->sl_iter, sl);
    skiplistIteratorSeek(&iter->sl_iter, start_gno);
}

void gtidGaplogDeinitDataIterator(gtidGaplogDataIterator *iter) {
    skiplistDeinitIterator(&iter->sl_iter);
}

void gtidGaplogDataIteratorSeek(gtidGaplogDataIterator *iter, gno_t gno) {
    skiplistIteratorSeek(&iter->sl_iter, gno);
}

gno_t gtidGaplogDataGetGno(gtidGaplogDataIterator* iter) {
    skiplistNode *node = iter->sl_iter.next;
    if (node == NULL) return -1;
    return (gno_t)node->score;
}

gtidGaplogKeys* gtidGaplogDataNext(gtidGaplogDataIterator* iter) {
    skiplistNode *node = skiplistIteratorNext(&iter->sl_iter);
    if (node == NULL) return NULL;
    return (gtidGaplogKeys*)node->value;
}

/* ========== gtidGaplog History iterator ========== */
void gtidGaplogInitHistoryIterator(gtidGaplogHistoryIterator* iter,
                                    gtidGaplog* gaplog, long long index) {
    iter->history = gaplog->history;
    gtidGaplogHistoryIteratorSeek(iter, index);
}

gno_t gtidGaplogHistoryNext(gtidGaplogHistoryIterator* iter,
                             const char** uuid, size_t* uuid_len) {
    if (iter->list_node == NULL) {
        *uuid = NULL;
        *uuid_len = 0;
        return 0;
    }

    uuidSet *us = listNodeValue(iter->list_node);
    *uuid = us->uuid;
    *uuid_len = us->uuid_len;

    gno_t result = iter->next_gno;

    iter->next_gno++;

    if (iter->next_gno > iter->interval_node->end) {
        iter->interval_node = iter->interval_node->forwards[0];
        if (iter->interval_node) {
            iter->next_gno = iter->interval_node->start;
        } else {
            iter->list_node = listNextNode(iter->list_node);
            if (iter->list_node) {
                us = listNodeValue(iter->list_node);
                iter->interval_node = us->intervals->header->forwards[0];
                iter->next_gno = iter->interval_node->start;
            }
        }
    }
    return result;
}

void gtidGaplogDeinitHistoryIterator(gtidGaplogHistoryIterator* iter) {
    UNUSED(iter);
}

/* Reposition the history iterator so the next call to
 * gtidGaplogHistoryNext returns the entry at the given `index` (0-based
 * position within the gap log's gno sequence). Walks across uuidSets and
 * their intervals, skipping `index` entries. Always resets from the head
 * of the history list, so multiple calls work correctly. If `index` is
 * past the end, leave the iterator in the exhausted state (list_node = NULL). */
void gtidGaplogHistoryIteratorSeek(gtidGaplogHistoryIterator* iter, long long index) {
    /* Always reset from the head of the history list */
    iter->list_node = listFirst(iter->history);
    iter->interval_node = NULL;
    iter->next_gno = 0;
    if (iter->list_node == NULL) return;
    uuidSet *us = listNodeValue(iter->list_node);
    iter->interval_node = us->intervals->header->forwards[0];
    long long remaining = index;
    while (iter->list_node && remaining > 0) {
        us = listNodeValue(iter->list_node);
        gno_t us_count = uuidSetCount(us);
        if (remaining >= us_count) {
            remaining -= us_count;
            iter->list_node = listNextNode(iter->list_node);
            if (iter->list_node) {
                us = listNodeValue(iter->list_node);
                iter->interval_node = us->intervals->header->forwards[0];
            }
            continue;
        }
        break;
    }

    while (iter->interval_node && remaining > 0) {
        gno_t interval_len = iter->interval_node->end - iter->interval_node->start + 1;
        if (remaining >= interval_len) {
            remaining -= interval_len;
            iter->interval_node = iter->interval_node->forwards[0];
            continue;
        }
        break;
    }

    if (iter->list_node && iter->interval_node) {
        iter->next_gno = iter->interval_node->start + remaining;
    } else {
        iter->list_node = NULL;
    }
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

int gtidGaplogTrim(gtidGaplog* gap_log ,size_t size) {
    size_t count = 0;
    while (count < size) {
        listNode *first_ln = listFirst(gap_log->history);
        if (first_ln == NULL) return count;

        uuidSet *first_uuid_set = (uuidSet*)listNodeValue(first_ln);

        gno_t min_gno = 0;
        gtidIntervalNode *first_node = first_uuid_set->intervals->header->forwards[0];
        if (first_node == NULL) {
            listDelNode(gap_log->history, first_ln);
            continue;
        }
        min_gno = first_node->start;

        sds evict_uuid_sds = sdsnewlen(first_uuid_set->uuid, first_uuid_set->uuid_len);
        dictEntry *de = dictFind(gap_log->data, evict_uuid_sds);
        if (de != NULL) {
            skiplist *sl = dictGetVal(de);
            serverAssert(skiplistDelete(sl, min_gno));
            if (sl->length == 0) {
                dictDelete(gap_log->data, evict_uuid_sds);
            }
        } else {
            serverPanic("not find keysinfo in gtid_gap_log");
        }
        sdsfree(evict_uuid_sds);
        serverAssert((uuidSetRemove(first_uuid_set, min_gno, min_gno) > 0));
        if (uuidSetCount(first_uuid_set) == 0) {
            listDelNode(gap_log->history, first_ln);
        }
        gap_log->size--;
        count++;
        
    }
    return count;
}

skipType gtid_skip_type = {
    .freeValue = gtidGaplogKeysRelease
};

static inline skiplist* gtidGaplogFindSkiplist(gtidGaplog* gaplog, sds uuid) {
    dictEntry *de = dictFind(gaplog->data, uuid);
    return de ? dictGetVal(de) : NULL;
}

static skiplist* gtidGaplogFindOrCreateSkiplist(gtidGaplog* gaplog, sds uuid) {
    dictEntry *de = dictFind(gaplog->data, uuid);
    if (de == NULL) {
        skiplist *sl = skiplistCreate(&gtid_skip_type);
        sds uuid_key = sdsdup(uuid);
        dictAdd(gaplog->data, uuid_key, sl);
        return sl;
    }
    return dictGetVal(de);
}

static skiplist* gtidGaplogFindSkiplistWithUUID(gtidGaplog* gaplog, 
                                                 const char* uuid, size_t uuid_len) {
    sds key = sdsnewlen(uuid, uuid_len);
    dictEntry *de = dictFind(gaplog->data, key);
    sdsfree(key);
    serverAssert(de != NULL);
    return dictGetVal(de);
}

int gtidGaplogInsert(gtidGaplog* gaplog, sds uuid, gno_t gno, gtidGaplogKeys* keys) {

    skiplist *sl = gtidGaplogFindOrCreateSkiplist(gaplog, uuid);

    serverAssert(skiplistInsert(sl, gno, keys, 1) != 0);

    uuidSet *last_uuid_set = NULL;
    listNode *tail_ln = listLast(gaplog->history);
    if (tail_ln != NULL) {
        last_uuid_set = (uuidSet*)listNodeValue(tail_ln);
        if (last_uuid_set->uuid_len != sdslen(uuid) ||
            memcmp(last_uuid_set->uuid, uuid, sdslen(uuid)) != 0) {
            last_uuid_set = NULL;
        }
    }

    if (last_uuid_set != NULL) {
        uuidSetAdd(last_uuid_set, gno, gno);
    } else {
        uuidSet *new_uuid_set = uuidSetNew(uuid, sdslen(uuid));
        uuidSetAdd(new_uuid_set, gno, gno);
        listAddNodeTail(gaplog->history, new_uuid_set);
    }

    gaplog->size++;
    
    if (gaplog->size >
            (size_t)server.gtid_xsync_max_gap) {
        gtidGaplogTrim(gaplog,
                        gaplog->size -
                        server.gtid_xsync_max_gap);
    }
    return 1;
}

int gtidGaplogDeleteRange(gtidGaplog* gaplog, sds uuid, gno_t start_gno, gno_t end_gno) {
    serverAssert(start_gno <= end_gno);

    long long deleted = 0;
    skiplist *sl = gtidGaplogFindSkiplist(gaplog, uuid);
    if (sl != NULL) {

        gtidGaplogDataIterator iter;
        gtidGaplogDataInitIterator(&iter, sl, start_gno);
        gno_t gno = -1;
        while ((gno = gtidGaplogDataGetGno(&iter)) != -1 && gno <= end_gno) {
            gtidGaplogDataNext(&iter);
            if (skiplistDelete(sl, gno)) {
                deleted++;
            }
        }
        gtidGaplogDeinitDataIterator(&iter);
        if (sl->length == 0) {
            dictDelete(gaplog->data, uuid);
        }
        gaplog->size -= deleted;
    }

    gno_t history_removed = 0;
    listNode *ln = listFirst(gaplog->history);
    while (ln) {
        uuidSet *us = listNodeValue(ln);
        listNode *next_ln = listNextNode(ln);
        if (us->uuid_len == sdslen(uuid) &&
            memcmp(us->uuid, uuid, sdslen(uuid)) == 0) {
            history_removed += uuidSetRemove(us, start_gno, end_gno);
            if (uuidSetCount(us) == 0) {
                listDelNode(gaplog->history, ln);
            }
        }
        ln = next_ln;
    }

    serverAssert(history_removed == deleted);
    return deleted;
}

int gtidGaplogQueryRange(gtidGaplog* gaplog, sds uuid, gno_t start_gno, gno_t end_gno,
                         gtidGaplogQueryRangeCallbackFn callback, void* ctx) {
    serverAssert(start_gno <= end_gno);
    skiplist *sl = gtidGaplogFindSkiplist(gaplog, uuid);
    if (sl == NULL) {
        return 0;
    }

    long long count = 0;

    gtidGaplogDataIterator iter;
    gtidGaplogDataInitIterator(&iter, sl, start_gno);

    gno_t gno;
    while ((gno = gtidGaplogDataGetGno(&iter)) != -1 && gno <= end_gno) {
        gtidGaplogKeys *keys = gtidGaplogDataNext(&iter);
        callback(gno, keys, ctx);
        count++;
    }

    gtidGaplogDeinitDataIterator(&iter);

    return count;
}

size_t gtidGaplogSize(gtidGaplog* gaplog) {
    if (gaplog == NULL) {
        return 0;
    }
    return gaplog->size;
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

    gtidGaplogHistoryIterator hist_iter;
    gtidGaplogInitHistoryIterator(&hist_iter, gaplog, start_idx);

    gtidGaplogDataIterator data_iter;
    const char *last_uuid = NULL;
    skiplist *sl = NULL;
    int nreply = 0;

    while (nreply < count) {
        const char *uuid;
        size_t uuid_len;
        gno_t gno = gtidGaplogHistoryNext(&hist_iter, &uuid, &uuid_len);
        if (gno == 0) break;

        if (last_uuid != uuid) {
            sl = gtidGaplogFindSkiplistWithUUID(gaplog, uuid, uuid_len);
            gtidGaplogDeinitDataIterator(&data_iter);
            gtidGaplogDataInitIterator(&data_iter, sl, gno);
            last_uuid = uuid;
        } else if (gtidGaplogDataGetGno(&data_iter) != gno) {
            gtidGaplogDeinitDataIterator(&data_iter);
            gtidGaplogDataInitIterator(&data_iter, sl, gno);
        }

        serverAssert(gtidGaplogDataGetGno(&data_iter) == gno);
        gtidGaplogKeys *keys = gtidGaplogDataNext(&data_iter);

        callback(uuid, uuid_len, gno, keys, ctx);
        nreply++;
    }
    gtidGaplogDeinitDataIterator(&data_iter);
    gtidGaplogDeinitHistoryIterator(&hist_iter);
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
                    serverLog(LL_WARNING, "[gaplog] gtidGaplogFillFromGtidSet unexpected command %s", cmd_name);
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

        /* uuid-1: gno=10 (interval) */
        gno = gtidGaplogHistoryNext(&iter, &uuid, &uuid_len);
        test_assert(gno == 10);
        test_assert(memcmp(uuid, "uuid-1", 6) == 0);

        /* uuid-1: gno=11 */
        gno = gtidGaplogHistoryNext(&iter, &uuid, &uuid_len);
        test_assert(gno == 11);

        /* uuid-1: gno=12 */
        gno = gtidGaplogHistoryNext(&iter, &uuid, &uuid_len);
        test_assert(gno == 12);

        /* uuid-2: gno=100 (uuidSet) */
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

    return error;
}
#endif
