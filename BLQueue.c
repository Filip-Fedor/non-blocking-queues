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
        free(queue);
        return NULL;
    }

    atomic_store(&queue->head, node);
    atomic_store(&queue->tail, node);
    HazardPointer_initialize(&queue->hp);

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
    HazardPointer_finalize(&queue->hp);
    free(queue);
}

void BLQueue_push(BLQueue* queue, Value item)
{
    BLNode* tail_node;
    int push_idx;
    while (true) {
        tail_node = HazardPointer_protect(&queue->hp,
                    (const _Atomic(void*)*)&queue->tail);
        push_idx = atomic_fetch_add(&tail_node->push_idx, 1);
        
        if (push_idx < BUFFER_SIZE) {
            Value expected = EMPTY_VALUE;

            if(atomic_compare_exchange_strong(&tail_node->buffer[push_idx],
                &expected, item)) {
                    
                    break;
            }
        }
        else {
            BLNode* new_node = BLNode_new();
            if (new_node == NULL) {
                continue;
            }

            atomic_store(&new_node->buffer[0], item);
            atomic_store(&new_node->push_idx, 1);
            BLNode* expected_tail = tail_node;

            if (atomic_compare_exchange_strong(&queue->tail, 
                &expected_tail, new_node)) {

                    atomic_store(&expected_tail->next, new_node);
                    break;
            }
            else {
                free(new_node);
            }
        }
    }
    
    HazardPointer_clear(&queue->hp);
}

Value BLQueue_pop(BLQueue* queue)
{
    BLNode* head_node;
    int pop_idx;
    Value item;
    while (true) {
        head_node = HazardPointer_protect(&queue->hp,
                    (const _Atomic(void*)*)&queue->head);
        pop_idx = atomic_fetch_add(&head_node->pop_idx, 1);

        if (pop_idx < BUFFER_SIZE) {
            item = atomic_exchange(&head_node->buffer[pop_idx], TAKEN_VALUE);

            if (item == EMPTY_VALUE) {
                HazardPointer_clear(&queue->hp);
                continue;
            }
            else {
                HazardPointer_clear(&queue->hp);
                return item;
            }
        }
        else {
            BLNode* next_node = atomic_load(&head_node->next);

            if (next_node == NULL) {
                HazardPointer_clear(&queue->hp);
                return EMPTY_VALUE;
            }
            else {
                BLNode* expected_head = head_node;

                if (atomic_compare_exchange_strong(&queue->head, 
                    &expected_head, next_node)) {

                        HazardPointer_retire(&queue->hp, expected_head);
                }
            }
        }
        HazardPointer_clear(&queue->hp);
    }
}

bool BLQueue_is_empty(BLQueue* queue)
{
    BLNode* head = HazardPointer_protect(&queue->hp, 
                    (const _Atomic(void*)*)&queue->head);
    int push_idx = atomic_load(&head->push_idx);
    int pop_idx = atomic_load(&head->pop_idx);
    BLNode* next = atomic_load(&head->next);

    HazardPointer_clear(&queue->hp);

    return (push_idx == pop_idx) && (next == NULL);
}
