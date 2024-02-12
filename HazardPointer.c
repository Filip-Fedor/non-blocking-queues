#include <malloc.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <threads.h>

#include "HazardPointer.h"

thread_local int _thread_id = -1;
int _num_threads = -1;

void HazardPointer_register(int thread_id, int num_threads)
{
    _thread_id = thread_id;
    _num_threads = num_threads;
}

void HazardPointer_initialize(HazardPointer* hp)
{
    for (int i = 0; i < MAX_THREADS; i++) {
        atomic_store(&hp->pointer[i], NULL);
        hp->retired_count[i] = 0;
        hp->retired[i] = NULL;
    }
}

void HazardPointer_finalize(HazardPointer* hp)
{
    for (int i = 0; i < _num_threads; i++) {
        RetiredNode* node = hp->retired[i];
        RetiredNode* helper;
        while (node != NULL) {
            helper = node;
            node = node->next;
            free(helper->ptr);
            free(helper);
        }
        hp->retired_count[i] = 0;
    }
}

void* HazardPointer_protect(HazardPointer* hp, const _Atomic(void*)* atom)
{
    void* ptr;

    do {
        ptr = atomic_load(atom);
        atomic_store(&hp->pointer[_thread_id], ptr);
    } while (ptr != atomic_load(atom));

    return ptr;
}

void HazardPointer_clear(HazardPointer* hp)
{
    atomic_store(&hp->pointer[_thread_id], NULL);
}



void HazardPointer_retire(HazardPointer* hp, void* ptr)
{
    RetiredNode* node = (RetiredNode*)malloc(sizeof(RetiredNode));
    if (node == NULL) {
        return;
    }
    node->ptr = ptr;
    node->next = hp->retired[_thread_id];
    hp->retired[_thread_id] = node;
    hp->retired_count[_thread_id]++;

    if (hp->retired_count[_thread_id] > RETIRED_THRESHOLD) {
        node = hp->retired[_thread_id];
        RetiredNode* prev = NULL;

        while (node != NULL) {
            void* p = node->ptr;
            bool is_reserved = false;

            for (int j = 0; j < _num_threads; j++) {
                if (atomic_load(&hp->pointer[j]) == p) {
                    is_reserved = true;
                    break;
                }
            }
            
            if (!is_reserved) {
                if (prev != NULL) {
                    prev->next = node->next;
                }
                else {
                    hp->retired[_thread_id] = node->next;
                }
                RetiredNode* tmp = node;
                node = node->next;
                free(tmp->ptr);
                free(tmp);
                hp->retired_count[_thread_id]--;
            }
            else {
                prev = node;
                node = node->next;
            }
        }
    }
}
