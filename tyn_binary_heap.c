#include <stdio.h>
#include <stdlib.h>
#include "tyan.h"
#include "tyn_utils.h"
#include "tyn_binary_heap.h"

int tyn_binary_heap_heapify(tyn_binary_heap_t *binary_heap, size_t i) {
    register int l,r,largest;
    l = 2*i;
    r = 2*i +1;
    largest = ((l <= binary_heap->size && binary_heap->cmp(binary_heap->nodes[l], binary_heap->nodes[i])) ? l : i);
    if(r <= binary_heap->size && binary_heap->cmp(binary_heap->nodes[r], binary_heap->nodes[largest])) {
        largest = r;
    }
    if(largest != i) { 
        void *tmp;
        tmp = binary_heap->nodes[i];
        binary_heap->nodes[i] = binary_heap->nodes[largest];
        binary_heap->nodes[largest] = tmp;
        tyn_binary_heap_heapify(binary_heap,largest);
    }
    return TYN_OK;
}

tyn_binary_heap_t* tyn_binary_heap_create(size_t max_size, int (*cmp)(void *a, void *b)) {
    tyn_binary_heap_t *binary_heap = (tyn_binary_heap_t *)malloc( sizeof(tyn_binary_heap_t) );
    binary_heap->size = 0;
    binary_heap->max_size = max_size;
    binary_heap->cmp = cmp;
    binary_heap->nodes = (void **)malloc(sizeof(void *) * (binary_heap->max_size +1));
    return binary_heap;
}

int tyn_binary_heap_insert(tyn_binary_heap_t *binary_heap, void *key) {
    register int i;
    if (binary_heap->size >= binary_heap->max_size) {
        tyn_log(stderr, "tyn_binary_heap: can not insert, the heap is full");
        return TYN_ERROR;
    }
    i = ++(binary_heap->size);
    while (i > 1 && binary_heap->cmp(key, binary_heap->nodes[i/2])) {
        binary_heap->nodes[i] = binary_heap->nodes[i/2];
        i = i/2;
    }
    binary_heap->nodes[i] = key;
    return TYN_OK;
}

void* tyn_binary_heap_extract_max(tyn_binary_heap_t *binary_heap) {
    void *max;
    if(binary_heap->size >= 1) {
        max = binary_heap->nodes[1];
        binary_heap->nodes[1] = binary_heap->nodes[binary_heap->size--];
        tyn_binary_heap_heapify(binary_heap,1);
    }
    return max;
}

int tyn_binary_heap_delete(tyn_binary_heap_t *binary_heap, size_t i) {
    void *deleted;
    if (i > binary_heap->size || i < 1) {
        return 0;
    }
    deleted = binary_heap->nodes[i];
    binary_heap->nodes[i] = binary_heap->nodes[binary_heap->size--];
    tyn_binary_heap_heapify(binary_heap, i);
    return TYN_OK;
}

int tyn_binary_heap_destroy(tyn_binary_heap_t *binary_heap) {
    free(binary_heap->nodes);
    free(binary_heap);
    return TYN_OK;
}
/*
int cmp(void *a, void *b) {
    const int *aa = (int *)a;
    const int *bb = (int *)b;
    //printf("compare %d %d\n", *aa, *bb);
    //printf("r: %d\n", (*aa > *bb) - (*aa < *bb));
   return  (*aa < *bb) - (*aa > *bb);
}

//gcc tyn_binary_heap.c tyn_utils.c -o tyn_binary_heap_test
int main() {
    tyn_binary_heap_t * binary_heap = tyn_binary_heap_create(10, cmp);
    int a[] = {5,4,3,2,1,10,9,8,7,6};
    int i;
    for(i = 0; i < 10; i++) {
        tyn_binary_heap_insert(binary_heap, &a[i]);
    }
    int *b = tyn_binary_heap_extract_max(binary_heap);
    printf("max is %d, size: %d\n", *b, binary_heap->size);
    b = tyn_binary_heap_extract_max(binary_heap);
    printf("max is %d, size: %d\n", *b, binary_heap->size);
    b = tyn_binary_heap_extract_max(binary_heap);
    printf("max is %d, size: %d\n", *b, binary_heap->size);
    int c = 1;
    int e = 1;
    tyn_binary_heap_insert(binary_heap, &c);
    tyn_binary_heap_insert(binary_heap, &e);
    b = tyn_binary_heap_extract_max(binary_heap);
    printf("max is %d, size: %d\n", *b, binary_heap->size);
    int d=99;
    tyn_binary_heap_insert(binary_heap, &d);
    b = tyn_binary_heap_extract_max(binary_heap);
    printf("max is %d, size: %d\n", *b, binary_heap->size);
    

    //test binary_heap
}
*/