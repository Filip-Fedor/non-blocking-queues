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
    _Atomic(Value) buffer[BUFFER_SIZE];
    _Atomic(int) push_idx;
    _Atomic(int) pop_idx;
};

// TODO BLNode_new

BLNode* BLNode_new() {
    BLNode* new_node = (BLNode*)malloc(sizeof(BLNode));
    if (new_node == NULL) {
        return NULL;
    }

    atomic_init(&new_node->next, NULL);
    for (int i = 0; i < BUFFER_SIZE; i++) {
        atomic_init(&new_node->buffer[i], EMPTY_VALUE);
    }

    atomic_init(&new_node->push_idx, 0);
    atomic_init(&new_node->pop_idx, 0);

    return new_node;
}

struct BLQueue {
    AtomicBLNodePtr head;
    AtomicBLNodePtr tail;
    HazardPointer hp;
};

BLQueue* BLQueue_new(void)
{
    BLQueue* queue = (BLQueue*)malloc(sizeof(BLQueue));
    if (queue == NULL) {
        return NULL;
    }

    BLNode* node = BLNode_new();
    if (node == NULL) {
        return NULL;
    }

    atomic_store(&queue->head, node);
    atomic_store(&queue->tail, node);

    return queue;
}

void BLQueue_delete(BLQueue* queue)
{
    BLNode* current = atomic_load(&queue->head);

    while(current != NULL) {
        BLNode* next = atomic_load(&current->next);
        free(current);
        current = next;
    }

    free(queue);
}

void BLQueue_push(BLQueue* queue, Value item)
{
    BLNode* tail_node;
    int push_idx;
    while (true) {
        tail_node = atomic_load(&queue->tail);
        push_idx = atomic_load(&tail_node->push_idx);
        atomic_fetch_add(&tail_node->push_idx, 1);
        
        if (push_idx < BUFFER_SIZE) {
            if (atomic_load(&tail_node->buffer[push_idx]) == TAKEN_VALUE) {
                continue;
            }
            else {
                atomic_store(&tail_node->buffer[push_idx], item);
                break;
            }
        }
        else {
            BLNode* new_node = BLNode_new();
            atomic_store(&new_node->buffer[0], item);
            atomic_store(&new_node->push_idx, 1);
            BLNode* expected_tail = tail_node;
            if (atomic_compare_exchange_strong(&queue->tail, &expected_tail, new_node)) {
                atomic_store(&expected_tail->next, new_node);
                break;
            }
            else {
                free(new_node);
            }
        }
    }
}

Value BLQueue_pop(BLQueue* queue)
{
    BLNode* head_node;
    int pop_idx;
    Value item;
    while (true) {
        head_node = atomic_load(&queue->head);
        pop_idx = atomic_load(&head_node->pop_idx);
        atomic_fetch_add(&head_node->pop_idx, 1);

        if (pop_idx < BUFFER_SIZE) {
            item = atomic_load(&head_node->buffer[pop_idx]);
            if (item == EMPTY_VALUE) {
                continue;
            }
            else {
                atomic_store(&head_node->buffer[pop_idx], TAKEN_VALUE);
                return item;
            }
        }
        else {
            BLNode* next_node = atomic_load(&head_node->next);
            if (next_node == NULL) {
                return EMPTY_VALUE;
            }
            else {
                BLNode* expected_head = head_node;
                if (atomic_compare_exchange_strong(&queue->head, 
                    &expected_head, next_node)) {
                           free(expected_head);
                    }
            }
        }
    }


    return EMPTY_VALUE; 
}

bool BLQueue_is_empty(BLQueue* queue)
{

    BLNode* node_head = atomic_load(&queue->head);
    bool is_empty = false;

    while (atomic_load(&node_head->push_idx) ==
            atomic_load(&node_head->pop_idx)) {
        
        node_head = atomic_load(&node_head->next);
        if (node_head == NULL) {
            is_empty = true;
            break;
        }

    }
    
    return is_empty;
}
