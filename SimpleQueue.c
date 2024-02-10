#include <malloc.h>
#include <pthread.h>
#include <stdatomic.h>

#include "SimpleQueue.h"

struct SimpleQueueNode;
typedef struct SimpleQueueNode SimpleQueueNode;

struct SimpleQueueNode {
    _Atomic(SimpleQueueNode*) next;
    Value item;
};

SimpleQueueNode* SimpleQueueNode_new(Value item)
{
    SimpleQueueNode* node = (SimpleQueueNode*)malloc(sizeof(SimpleQueueNode));
    if (node == NULL) {
        return NULL;
    }

    atomic_init(&node->next, NULL);
    node->item = item;

    return node;
}

struct SimpleQueue {
    SimpleQueueNode* head;
    SimpleQueueNode* tail;
    pthread_mutex_t head_mtx;
    pthread_mutex_t tail_mtx;
};

SimpleQueue* SimpleQueue_new(void)
{
    SimpleQueue* queue = (SimpleQueue*)malloc(sizeof(SimpleQueue));
    if (queue == NULL) {
        return NULL;
    }

    SimpleQueueNode* dummy_node = SimpleQueueNode_new(EMPTY_VALUE);
    if (dummy_node == NULL) {
        free(queue);
        return NULL;
    }

    queue->head = dummy_node;
    queue->tail = dummy_node;

    pthread_mutex_init(&queue->head_mtx, NULL);
    pthread_mutex_init(&queue->tail_mtx, NULL);

    return queue;
}

void SimpleQueue_delete(SimpleQueue* queue)
{
    SimpleQueueNode* node = queue->head;
    while (node != NULL) {
        SimpleQueueNode* tmp = atomic_load(&node->next);
        free(node);
        node = tmp;
    }

    pthread_mutex_destroy(&queue->head_mtx);
    pthread_mutex_destroy(&queue->tail_mtx);
    free(queue);
}

void SimpleQueue_push(SimpleQueue* queue, Value item)
{
    pthread_mutex_lock(&queue->tail_mtx);

    SimpleQueueNode* new_node = SimpleQueueNode_new(item);
    atomic_store(&queue->tail->next, new_node);
    queue->tail = new_node;

    pthread_mutex_unlock(&queue->tail_mtx);
}

Value SimpleQueue_pop(SimpleQueue* queue)
{
    pthread_mutex_lock(&queue->head_mtx);

    Value item = EMPTY_VALUE;
    SimpleQueueNode* head_node = queue->head;
    SimpleQueueNode* pop_node = atomic_load(&queue->head->next);
    
    if (pop_node == NULL) {
        pthread_mutex_unlock(&queue->head_mtx);
        return EMPTY_VALUE;
    }

    item = pop_node->item;
    queue->head = pop_node;

    free(head_node);

    pthread_mutex_unlock(&queue->head_mtx);

    return item;
}

bool SimpleQueue_is_empty(SimpleQueue* queue)
{
    pthread_mutex_lock(&queue->head_mtx);
    
    bool is_empty = (atomic_load(&queue->head->next) == NULL);

    pthread_mutex_unlock(&queue->head_mtx);

    return is_empty;
}
