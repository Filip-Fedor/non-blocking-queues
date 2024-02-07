#include <malloc.h>
#include <stdatomic.h>
#include <stdbool.h>

#include "HazardPointer.h"
#include "LLQueue.h"

struct LLNode;
typedef struct LLNode LLNode;
typedef _Atomic(LLNode*) AtomicLLNodePtr;

struct LLNode {
    AtomicLLNodePtr next;
    // TODO
};

LLNode* LLNode_new(Value item)
{
    LLNode* node = (LLNode*)malloc(sizeof(LLNode));
    // TODO
    return node;
}

struct LLQueue {
    AtomicLLNodePtr head;
    AtomicLLNodePtr tail;
    HazardPointer hp;
};

LLQueue* LLQueue_new(void)
{
    LLQueue* queue = (LLQueue*)malloc(sizeof(LLQueue));
    // TODO
    return queue;
}

void LLQueue_delete(LLQueue* queue)
{
    // TODO
    free(queue);
}

void LLQueue_push(LLQueue* queue, Value item)
{
    // TODO
}

Value LLQueue_pop(LLQueue* queue)
{
    return EMPTY_VALUE; // TODO
}

bool LLQueue_is_empty(LLQueue* queue)
{
    return false; // TODO
}
