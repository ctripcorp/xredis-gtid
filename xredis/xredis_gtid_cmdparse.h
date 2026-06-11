#ifndef XREDIS_GTID_CMDPARSE_H
#define XREDIS_GTID_CMDPARSE_H

typedef struct redisObject robj;

typedef enum {
    CMDPARSE_EXTRA_NONE = 0,
    CMDPARSE_EXTRA_RANGE,     /* list range: start/end/reverse */
    CMDPARSE_EXTRA_ZSCORE,    /* zset score range: min/max/minex/maxex/reverse */
    CMDPARSE_EXTRA_ZLEX,      /* zset lex range: min_arg_idx/max_arg_idx/minex/maxex */
    CMDPARSE_EXTRA_ZRANK,     /* zset rank range: start/end */
    CMDPARSE_EXTRA_BITOFF,    /* bitmap offset */
    CMDPARSE_EXTRA_BITRNG,    /* bitmap range: start/end */
} cmdParseExtraType;

/* TODO */
typedef struct cmdParseKeyExtra {
    cmdParseExtraType extra_type;
    union {
        struct {
            long long start;
            long long end;
            int reverse;
        } range; /* unuse，swap list need arg_rewrite0, arg_rewrite1*/
        struct {
            double min;
            double max;
            int minex;
            int maxex;
            int reverse;
        } zscore; /* unuse */
        struct {
            int min_arg_idx;
            int max_arg_idx;
            int minex;
            int maxex;
        } zlex; /* unuse (need swap zset all) */
        struct {
            long long start;
            long long end;
        } zrank; /* unuse (need swap zset all) */
    };
} cmdParseKeyExtra;

struct redisCommand;
typedef void (*cmdParseOnKeyFn)(void *ctx, int dbid, struct redisCommand* cmd, robj** argv, int argc, int key_arg_idx,
                                int subkeys_count, int subkeys_start,
                                int subkeys_step, const int *subkey_arg_idxs,
                                const cmdParseKeyExtra *extra);

typedef struct cmdParseCommandDef {
    const char *name;
    void (*parse)(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key);
} cmdParseCommandDef;



int cmdParseCountKeys(struct redisCommand *cmd, robj **argv, int argc);

void cmdParseKeys(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key);



int (*cmdParseGetCountFunc(const char *cmd_name))(robj **argv, int argc);

void (*cmdParseGetParseFunc(const char *cmd_name))(int dbid, struct redisCommand *cmd, robj **argv, int argc, void *ctx, cmdParseOnKeyFn on_key);

#endif
