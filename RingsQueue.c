#include <malloc.h>
#include <pthread.h>
#include <stdatomic.h>

#include <stdio.h>
#include <stdlib.h>

#include "HazardPointer.h"
#include "RingsQueue.h"

struct RingsQueueNode;
typedef struct RingsQueueNode RingsQueueNode;

struct RingsQueueNode {
    _Atomic(RingsQueueNode*) next;
    // TODO
};

// TODO RingsQueueNode_new

struct RingsQueue {
    RingsQueueNode* head;
    RingsQueueNode* tail;
    pthread_mutex_t pop_mtx;
    pthread_mutex_t push_mtx;
};

RingsQueue* RingsQueue_new(void)
{
    RingsQueue* queue = (RingsQueue*)malloc(sizeof(RingsQueue));
    // TODO
    return queue;
}

void RingsQueue_delete(RingsQueue* queue)
{
    // TODO
    free(queue);
}

void RingsQueue_push(RingsQueue* queue, Value item)
{
    // TODO
}

Value RingsQueue_pop(RingsQueue* queue)
{
    return EMPTY_VALUE; // TODO
}

bool RingsQueue_is_empty(RingsQueue* queue)
{
    return false; // TODO
}
