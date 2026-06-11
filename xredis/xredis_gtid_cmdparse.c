#include "xredis_gtid_cmdparse.h"
#include "xredis_gtid_adaptation_version.h"
#include "server.h"

/* --- hset / hmset：one key + subkeys（field）, step 2, from argv[2] start  --- */
static void cmdParseHset(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    int subkeys_count = (argc - 2) / 2;
    on_key(ctx, dbid, cmd, argv, argc, 1, subkeys_count, 2, 2, NULL, NULL);
}

/* ---one key + member subkeys, step 1, from argv[2] start --- */
static void cmdParseSingleKeySubkeys(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    int subkeys_count = argc - 2;
    on_key(ctx, dbid, cmd, argv, argc, 1, subkeys_count, 2, 1, NULL, NULL);
}

/* --- hsetnx / hincrby / hincrbyfloat：one key + one subkey --- */
static void cmdParseOneSubkey(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    serverAssert(argc >= 3);
    on_key(ctx, dbid, cmd, argv, argc, 1, 1, 2, 1, NULL, NULL);
}



/* --- zadd：one key + member subkeys, step 2 --- */
static void cmdParseZadd(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    int i = 2;
    while (i < argc) {
        sds arg = (sds)argv[i]->ptr;
        if (!strcasecmp(arg, "nx") || !strcasecmp(arg, "xx") ||
            !strcasecmp(arg, "ch") || !strcasecmp(arg, "incr") ||
            !strcasecmp(arg, "gt") || !strcasecmp(arg, "lt")) {
            i++;
        } else {
            break;
        }
    }
    int subkeys_count = (argc - i) / 2;
    on_key(ctx, dbid, cmd, argv, argc, 1, subkeys_count, i + 1, 2, NULL, NULL);
}


/* --- zincrby：one key + one subkey --- */
static void cmdParseZincrby(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    UNUSED(dbid);
    serverAssert(argc >= 4);
    on_key(ctx, dbid, cmd, argv, argc, 1, 1, 3, 1, NULL, NULL);
}


/* 3 step */
static void cmdParseGeoAdd(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    int i = 2;
    while (i < argc) {
        sds arg = (sds)argv[i]->ptr;
        if (!strcasecmp(arg, "nx") || !strcasecmp(arg, "xx") || !strcasecmp(arg, "ch")) {
            i++;
        } else {
            break;
        }
    }
    int subkeys_count = (argc - i) / 3;  /* lon/lat/member  */
    on_key(ctx, dbid, cmd, argv, argc, 1, subkeys_count, i + 2, 3, NULL, NULL); /* member starts at argv[i+2] */
}

/* geodist：key + member1 + member2（ 2  subkey） */
static void cmdParseGeoDist(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    serverAssert(argc >= 4);
    on_key(ctx, dbid, cmd, argv, argc, 1, 2, 2, 1, NULL, NULL);
}




#define cmdParseZrevrange cmdParseZrange

#include "xredis_commands.def"

dictType cmd_parse_command_dict_type = {
    dictSdsHash,
    NULL,
    NULL,
    dictSdsKeyCompare,
    dictSdsDestructor,
    NULL,
};

dict* createCmdParseCommandDict() {
    dict* cmd_parse_command_dict = gtidDictCreate(&cmd_parse_command_dict_type);
    for (int i = 0; cmd_parse_commands[i].name != NULL; i++) {
        dictAdd(cmd_parse_command_dict, sdsnew(cmd_parse_commands[i].name), &cmd_parse_commands[i]);
    }
    return cmd_parse_command_dict;
}
void cmdParseKeys(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key) {
    static dict* cmd_parse_command_dict = NULL;
    if (cmd_parse_command_dict == NULL) {
        cmd_parse_command_dict = createCmdParseCommandDict();
    }
    if (argc < 1) return;
    cmdParseCommandDef* parsecmd = dictFetchValue(cmd_parse_command_dict,argv[0]->ptr);
    if (parsecmd != NULL) {
        parsecmd->parse(dbid, cmd, argv, argc, ctx, on_key);
        return;
    }
    if (cmd == NULL) cmd = gtidLookupCommandBySds(argv[0]->ptr);
    serverAssert(cmd != NULL);
    getKeysResult keys = GETKEYS_RESULT_INIT;
    int numkeys = getKeysFromCommand(cmd, argv, argc, &keys);
    for (int i = 0; i < numkeys; i++) {
        on_key(ctx, dbid, cmd, argv, argc, gtidGetKeysResultKeyIndex(&keys, i), 0, 0, 0, NULL, NULL);
    }
    getKeysFreeResult(&keys);
    return;
}


