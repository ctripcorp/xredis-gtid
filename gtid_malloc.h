#ifndef GTID_MALLOC_H
#define GTID_MALLOC_H
#include <stdlib.h>
#define GTID_ALLOC_LIB "libc"
#define gtid_malloc malloc
#define gtid_realloc realloc
#define gtid_free free
#endif

