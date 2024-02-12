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
    Value item;
};

LLNode* LLNode_new(Value item)
{
    LLNode* node = (LLNode*)malloc(sizeof(LLNode));
    if (node == NULL) {
        return NULL;
    }

    atomic_init(&node->next, NULL);
    node->item = item;

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
    if (queue == NULL) {
        return NULL;
    }

    LLNode* new_node = LLNode_new(EMPTY_VALUE);
    if (new_node == NULL) {
        free(queue);
        return NULL;
    }

    atomic_store(&queue->head, new_node);
    atomic_store(&queue->tail, new_node);
    HazardPointer_initialize(&queue->hp);

    return queue;
}

void LLQueue_delete(LLQueue* queue)
{
    LLNode* node;

    while ((node = atomic_load(&queue->head)) != NULL) {
        LLNode* next = atomic_load(&node->next);
        free(node);
        atomic_store(&queue->head, next);
    }

    HazardPointer_finalize(&queue->hp);
    free(queue);
}

void LLQueue_push(LLQueue* queue, Value item)
{
    LLNode* new_node = LLNode_new(item);
    LLNode* tail;
    LLNode* next;
    
    while (true) {
        tail = HazardPointer_protect(&queue->hp, 
                                    (const _Atomic(void*)*)&queue->tail);
        next = atomic_load(&tail->next);

        if (tail == atomic_load(&queue->tail)) {
            if (next == NULL) {
                if (atomic_compare_exchange_strong(&tail->next, 
                                                    &next, new_node)) {
                    atomic_compare_exchange_strong(&queue->tail,
                                                    &tail, new_node);
                    HazardPointer_clear(&queue->hp);
                    return;
                }
            }
            else {
                atomic_compare_exchange_strong(&queue->tail, &tail, next);
            }
        }
        HazardPointer_clear(&queue->hp);
    }
}

Value LLQueue_pop(LLQueue* queue)
{
    LLNode* head;
    LLNode* tail;
    LLNode* next;
    Value item;

    while (true) {
        head = HazardPointer_protect(&queue->hp, 
                (const _Atomic(void*)*)&queue->head);
        tail = atomic_load(&queue->tail);
        next = atomic_load(&head->next);

        if (head == atomic_load(&queue->head)) {
            if (head == tail) {
                if (next == NULL) {
                    HazardPointer_clear(&queue->hp);
                    return EMPTY_VALUE;
                }
                atomic_compare_exchange_strong(&queue->tail, &tail, next);
            }
            else {
                HazardPointer_protect(&queue->hp, 
                                    (const _Atomic(void*)*)&next);
                if (next != NULL) {
                    if (next != atomic_load(&queue->head->next)) {
                        HazardPointer_clear(&queue->hp);
                        continue;
                    }
                    item = next->item;
                    if (atomic_compare_exchange_strong(&queue->head,
                                                    &head, next)) {
                        HazardPointer_retire(&queue->hp, head);
                        HazardPointer_clear(&queue->hp);
                        return item;
                    }
                }
            }
        }
    }
}

bool LLQueue_is_empty(LLQueue* queue)
{
    LLNode* next = atomic_load(&queue->head->next);

    return next == NULL;
}
