

#define _DEFAULT_SOURCE  
#include <gtid.h>

#include <stdlib.h>
#include <stdio.h>
#include <gtid_malloc.h>
#include <math.h>

static skiplistNode *skiplistNodeCreate(int level, long long score,
                                                     void *value) {
    skiplistNode *node = gtid_malloc(sizeof(skiplistNode) +
                                       sizeof(node->level[0]) * level);
    node->score = score;
    node->value = value;
    node->backward = NULL;
    for (int i = 0; i < level; i++)
        node->level[i].forward = NULL;
    return node;
}

static void skiplistNodeFree(skiplist* sl, skiplistNode *node) {
    if (node->value) sl->type->freeValue(node->value);
    gtid_free(node);
}
skiplist* skiplistCreate(skipType* type) {
    skiplist *sl = gtid_malloc(sizeof(skiplist));
    sl->level = 1;
    sl->length = 0;
    sl->tail = NULL;
    sl->type = type;
    sl->header = skiplistNodeCreate(SKIPLIST_MAXLEVEL, 0, NULL);
    return sl;
}

void skiplistFree(skiplist *sl) {
    skiplistNode *node = sl->header->level[0].forward;
    gtid_free(sl->header);
    while (node) {
        skiplistNode *next = node->level[0].forward;
        skiplistNodeFree(sl,node);
        node = next;
    }
    gtid_free(sl);
}

static int skiplistRandomLevel(void) {
    int level = 1;
    while (level < SKIPLIST_MAXLEVEL && (random() & 0x3) == 0)
        level++;
    return level;
}

int skiplistInsert(skiplist *sl, long long score,
                           void *value, int score_unique) {
    skiplistNode *update[SKIPLIST_MAXLEVEL];
    skiplistNode *x = sl->header;

    for (int i = sl->level - 1; i >= 0; i--) {
        while (x->level[i].forward && x->level[i].forward->score < score) 
            x = x->level[i].forward;
        if ( score_unique && x->level[i].forward && (x->level[i].forward->score == score)) {
            return 0;
        }    
        update[i] = x;
    }
    
    
    int level = skiplistRandomLevel();
    if (level > sl->level) {
        for (int i = sl->level; i < level; i++)
            update[i] = sl->header;
        sl->level = level;
    }

    x = skiplistNodeCreate(level, score, value);
    for (int i = 0; i < level; i++) {
        x->level[i].forward = update[i]->level[i].forward;
        update[i]->level[i].forward = x;
    }

    x->backward = (update[0] == sl->header) ? NULL : update[0];
    if (x->level[0].forward)
        x->level[0].forward->backward = x;
    else
        sl->tail = x;

    sl->length++;
    return 1;
}

int skiplistDelete(skiplist *sl, long long score) {
    skiplistNode* update[SKIPLIST_MAXLEVEL];
    skiplistNode* x = sl->header;

    for (int i = sl->level - 1; i >= 0; i--) {
        while (x->level[i].forward && x->level[i].forward->score < score)
            x = x->level[i].forward;
        update[i] = x;
    }

    x = x->level[0].forward;
    if (x == NULL || x->score != score) return 0;

    for (int i = 0; i < sl->level; i++) {
        if (update[i]->level[i].forward != x) break;
        update[i]->level[i].forward = x->level[i].forward;
    }

    if (x->level[0].forward)
        x->level[0].forward->backward = x->backward;
    else
        sl->tail = x->backward;

    while (sl->level > 1 && sl->header->level[sl->level - 1].forward == NULL)
        sl->level--;

    skiplistNodeFree(sl, x);
    sl->length--;
    return 1;
}

skiplistNode* skiplistFirst(skiplist *sl) {
    return sl->header->level[0].forward;
}

skiplistNode* skiplistFindFirstGte(skiplist *sl, long long target) {
    skiplistNode *x = sl->header;
    for (int i = sl->level - 1; i >= 0; i--) {
        while (x->level[i].forward && x->level[i].forward->score < target)
            x = x->level[i].forward;
    }
    return x->level[0].forward;
}

int skiplistInitIterator(skiplistIterator *it, skiplist *sl) {
    it->sl = sl;
    it->next = sl->header->level[0].forward;
    it->reverse = 0;
    return 0;
}

int skiplistReverseInitIterator(skiplistIterator *it, skiplist *sl) {
    it->sl = sl;
    it->next = sl->tail;
    it->reverse = 1;
    return 0;
}

void skiplistDeinitIterator(skiplistIterator *it) {
    /* No heap allocations; nothing to release. The sl pointer is owned by
     * the caller. Kept for API symmetry with the other iterators in
     * this codebase. */
    (void)it;
}

skiplistNode *skiplistIteratorNext(skiplistIterator *it) {
    skiplistNode *curr = it->next;
    if (curr == NULL) return NULL;
    it->next = it->reverse ? curr->backward : curr->level[0].forward;
    return curr;
}

int skiplistIteratorSeek(skiplistIterator *it, long long target) {
    skiplistNode *node = skiplistFindFirstGte(it->sl, target);
    if (node == NULL) {
        it->next = NULL;
        return 0;
    }
    it->next = node;
    return 1;
}