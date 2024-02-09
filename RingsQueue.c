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
    Value buffer[RING_SIZE];
    _Atomic(int) push_idx;
    _Atomic(int) pop_idx;
};

RingsQueueNode* RingsQueueNode_new() {
    RingsQueueNode* node = (RingsQueueNode*)malloc(sizeof(RingsQueueNode));
    if (node == NULL) {
        return NULL;
    }

    atomic_init(&node->next, NULL);
    atomic_init(&node->push_idx, 0);
    atomic_init(&node->pop_idx, 0);

    return node;
}

struct RingsQueue {
    RingsQueueNode* head;
    RingsQueueNode* tail;
    pthread_mutex_t pop_mtx;
    pthread_mutex_t push_mtx;
};

RingsQueue* RingsQueue_new(void)
{
    RingsQueue* queue = (RingsQueue*)malloc(sizeof(RingsQueue));
    if (queue == NULL) {
        return NULL;
    }

    RingsQueueNode* new_node = RingsQueueNode_new();
    if (new_node == NULL) {
        free(queue);
        return NULL;
    }

    queue->head = new_node;
    queue->tail = new_node;
    pthread_mutex_init(&queue->pop_mtx, NULL);
    pthread_mutex_init(&queue->push_mtx, NULL);

    return queue;
}

void RingsQueue_delete(RingsQueue* queue)
{
    RingsQueueNode* node = queue->head;
    while (node != NULL) {
        RingsQueueNode* tmp = atomic_load(&node->next);
        free(node);
        node = tmp;
    }

    pthread_mutex_destroy(&queue->pop_mtx);
    pthread_mutex_destroy(&queue->push_mtx);
    free(queue);
}

void RingsQueue_push(RingsQueue* queue, Value item)
{
    pthread_mutex_lock(&queue->push_mtx);

    RingsQueueNode* tail_node = queue->tail;
    int push_idx = atomic_load(&tail_node->push_idx);
    int pop_idx = atomic_load(&tail_node->pop_idx);

    if (push_idx - pop_idx < RING_SIZE) {
        int idx = push_idx % RING_SIZE;
        tail_node->buffer[idx] = item;
        atomic_fetch_add(&tail_node->push_idx, 1);
    }
    else {
        RingsQueueNode* new_node = RingsQueueNode_new();
        new_node->buffer[0] = item;
        atomic_fetch_add(&new_node->push_idx, 1);
        atomic_store(&tail_node->next, new_node);
        queue->tail = new_node;
    }    

    pthread_mutex_unlock(&queue->push_mtx);
}

Value RingsQueue_pop(RingsQueue* queue)
{
    pthread_mutex_lock(&queue->pop_mtx);

    RingsQueueNode* head_node;
    RingsQueueNode* next_node;
    int pop_idx;

    while (true) {
        head_node = queue->head;
        pop_idx = atomic_load(&head_node->pop_idx);
        if (pop_idx == atomic_load(&head_node->push_idx)) {
            next_node = atomic_load(&head_node->next);
            if (next_node == NULL) {
                pthread_mutex_unlock(&queue->pop_mtx);
                return EMPTY_VALUE;
            }
            queue->head = next_node;
            free(head_node);
        }
        else {
            break;
        }
    }

    int idx = pop_idx % RING_SIZE;
    Value item = head_node->buffer[idx];
    atomic_fetch_add(&head_node->pop_idx, 1);

    pthread_mutex_unlock(&queue->pop_mtx);

    return item;
}

bool RingsQueue_is_empty(RingsQueue* queue)
{
    pthread_mutex_lock(&queue->pop_mtx);

    RingsQueueNode* head_node = queue->head;
    bool is_empty = false;
    while (atomic_load(&head_node->push_idx) == 
            atomic_load(&head_node->pop_idx)) {

        head_node = atomic_load(&head_node->next);
        if (head_node == NULL) {
            is_empty = true;
            break;
        }
    }

    pthread_mutex_unlock(&queue->pop_mtx);

    return is_empty;
}
