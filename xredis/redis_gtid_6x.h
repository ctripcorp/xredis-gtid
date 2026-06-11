

#ifndef GTID_REDIS_H
#define GTID_REDIS_H

void afterErrorReply(client *c, const char *s, size_t len);
int masterTryPartialResynchronization(client *c);
#endif