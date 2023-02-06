#include <stdio.h>
#include <assert.h>
#include "gtid.h"

/* Arrange the N elements of ARRAY in random order.
   Only effective if N is much smaller than RAND_MAX;
   if this may not be the case, use a better random
   number generator. */
void shuffle(int *array, size_t n)
{
    if (n > 1)
    {
        size_t i;
        for (i = 0; i < n - 1; i++)
        {
          size_t j = i + rand() / (RAND_MAX / (n - i) + 1);
          int t = array[j];
          array[j] = array[i];
          array[i] = t;
        }
    }
}

int main(int argc, char* argv[]) {
    size_t window, count;
    if (argc != 3) {
        printf("%s <window> <count>\n", argv[0]);
        exit(1);
    }

    window = atoll(argv[1]);
    count = atoll(argv[2]);

    int *array = malloc(sizeof(gno_t)*window);
    for (size_t i = 0; i < window; i++)
        array[i] = i;

    shuffle(array, window);

    uuidSet *uuid_set = uuidSetNew("A", 1);
    for (gno_t gno = 1; gno+window <= count; gno += window) {
        for (int i = 0; i < window; i++) {
            gno_t cur = gno + array[i];
            uuidSetAdd(uuid_set, cur, cur);
        }
        assert(uuid_set->intervals->node_count == 2);
        assert(uuid_set->intervals->gno_count == gno+window-1);
    }

    return 0;
}
