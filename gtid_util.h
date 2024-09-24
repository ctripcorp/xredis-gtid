#ifndef GTID_UTIL_H
#define GTID_UTIL_H

#include <stdlib.h>
#include <stdint.h>

uint32_t digits10(uint64_t v);
int ll2string(char *dst, size_t dstlen, long long svalue);
int string2ll(const char *s, size_t slen, long long *value);

#endif
