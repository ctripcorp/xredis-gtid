



#ifndef GTID_REDIS_H
#define GTID_REDIS_H


void afterErrorReply(client *c, const char *s, size_t len, int flags);
int masterTryPartialResynchronization(client *c, long long psync_offset);

#endif