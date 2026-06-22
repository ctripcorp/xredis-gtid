#ifndef GTID_ADAPTATION_VERSION_H
#define GTID_ADAPTATION_VERSION_H
#include "server.h"

/* dict */
dict* gtidDictCreate(dictType *type);

/* command */
struct redisCommand* gtidLookupCommandBySds(sds name);
int gitdCmdGetKeyType(struct redisCommand *cmd);
char* gtidRedisCommandGetName(struct redisCommand* cmd);

/* obj  */
char* gtidGetTypeName(int key_type);

/* getKeysResult*/
int gtidGetKeysResultKeyIndex(getKeysResult* result, int index);

/* replication */
long long gtidGetBacklogOffset();
long long gtidGetBacklogHistlen();
void ctrip_resizeReplicationBacklog(long long newsize);
void ctrip_replicationFeedSlaves(list* saves,int dictid, robj **argv,
        int argc, const char *uuid, size_t uuid_len, gno_t gno, long long offset);
void ctrip_replicationFeedSlavesFromMasterStream(list *slaves, char *buf, size_t buflen, const char *uuid, size_t uuid_len, gno_t gno, long long offset);
void ctrip_feedAppendOnlyFile(struct redisCommand *cmd, int dictid, robj **argv, int argc);

typedef void (*consume_cb)(char *p, long long thislen, void *pd);
long long consumeReplicationBacklogLimited(long long offset, long long limit,
        consume_cb cb, void *pd);
void consumeReplicationBacklogLimitedAddReplyCb(char *p,
        long long thislen, void *pd);
void gtidClearReplStartCmdStreamOnAck(client* c);
void gtidFreeClientAsync(client *c);
int gtidMasterTryPartialResynchronization(client* c, long long psync_offset) ;

void feedAppendOnlyFileGtid(struct redisCommand *_cmd, int dictid, robj **argv, int argc);
void ctrip_replicationFeedSlaves(list* saves,int dictid, robj **argv,
        int argc, const char *uuid, size_t uuid_len, gno_t gno, long long offset);

/* backlog */
long long gtidBacklogAppendToSds(long long offset, sds *dst, size_t size);
long long gtidGetReplBacklogOffset();
long long gtidGetReplBacklogHistlen();

/* client */
void gtidMockClientInit(client* c);
void gtidMockClientDeinit(client* c);
void gtidMockClientCleanArgv(client* c);
void gtidMockClientMoveClientArgv(client *c);
void gtidAfterErrorReply(client *c, const char *s, size_t len, int flags);

/* command */

int isExecCommand(struct redisCommand *cmd);
int isGtidCommand(struct redisCommand *cmd);
char* gtidGetCmdName(struct redisCommand* cmd);
struct redisCommand* gtidGetGtidCommand();
struct redisCommand* gtidGetExecCommand();

/* gtid test*/
void gtidInitTestEnv();
void gtidFeedReplicationBacklog(void* buffer, size_t len);
#endif