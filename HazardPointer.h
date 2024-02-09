#pragma once

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#define MAX_THREADS 128
#define RETIRED_THRESHOLD 10

typedef struct RetiredNode {
    void* ptr;
    struct RetiredNode* next;
} RetiredNode;

struct HazardPointer {
    _Atomic(void*) pointer[MAX_THREADS];
    RetiredNode* retired[MAX_THREADS];
    size_t retired_count[MAX_THREADS];
};
typedef struct HazardPointer HazardPointer;

void HazardPointer_register(int thread_id, int num_threads);
void HazardPointer_initialize(HazardPointer* hp);
void HazardPointer_finalize(HazardPointer* hp);
void* HazardPointer_protect(HazardPointer* hp, const _Atomic(void*)* atom);
void HazardPointer_clear(HazardPointer* hp);
void HazardPointer_retire(HazardPointer* hp, void* ptr);
