#include <malloc.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "BLQueue.h"
#include "HazardPointer.h"

struct BLNode;
typedef struct BLNode BLNode;
typedef _Atomic(BLNode*) AtomicBLNodePtr;

struct BLNode {
    AtomicBLNodePtr next;
    // TODO
};

// TODO BLNode_new

struct BLQueue {
    AtomicBLNodePtr head;
    AtomicBLNodePtr tail;
    HazardPointer hp;
};

BLQueue* BLQueue_new(void)
{
    BLQueue* queue = (BLQueue*)malloc(sizeof(BLQueue));
    // TODO
    return queue;
}

void BLQueue_delete(BLQueue* queue)
{
    // TODO
    free(queue);
}

void BLQueue_push(BLQueue* queue, Value item)
{
    // TODO
}

Value BLQueue_pop(BLQueue* queue)
{
    return EMPTY_VALUE; // TODO
}

bool BLQueue_is_empty(BLQueue* queue)
{
    return false; // TODO
}
