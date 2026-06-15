#include "server.h"
#include "xredis_gtid_adaptation_version.h"

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
