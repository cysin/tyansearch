#include <stdio.h>
#include <stdint.h>
#include "tyan.h"
#include "tyn_binary_heap.h"
#include "tyn_exsorter.h"



tyn_exsorter_t* tyn_exsorter_create(size_t buffer_size, size_t item_size, int (* cmp_func)(void *, void*), int (* node_cmp_func)(tyn_exsorter_node_t*, tyn_exsorter_node_t *),FILE *fp_tmp, FILE *fp_write) {
    tyn_exsorter_t *exsorter = (tyn_exsorter_t *)malloc( sizeof(tyn_exsorter_t) );
    exsorter->buffer = (char *)malloc( buffer_size );
    exsorter->buffer_cur = exsorter->buffer;
    exsorter->buffer_size = buffer_size;
    exsorter->item_size = item_size;
    exsorter->max_num_items = buffer_size / item_size;
    exsorter->num_items = 0;
    exsorter->num_blocks = 0;
    exsorter->cmp_func = cmp_func;
    exsorter->node_cmp_func = node_cmp_func;
    exsorter->fp_tmp = fp_tmp;
    exsorter->fp_write = fp_write;
    return exsorter;
}

int tyn_exsorter_destroy(tyn_exsorter_t *exsorter) {
    free(exsorter->buffer);
    close(exsorter->fp_tmp);
    close(exsorter->fp_write);
    free(exsorter);
}

int tyn_exsorter_add(void *item, tyn_exsorter_t *exsorter) {
    memcpy(exsorter->buffer_cur, item, exsorter->item_size);
    exsorter->num_items++;
    exsorter->buffer_cur += exsorter->item_size;
    if(exsorter->num_items == exsorter->max_num_items) {
        //flush to disk
        tyn_exsorter_flush(exsorter);
    }
}

int tyn_exsorter_flush(tyn_exsorter_t *exsorter) {
    qsort(exsorter->buffer, exsorter->num_items, exsorter->item_size, exsorter->cmp_func);
    size_t num_wrote = fwrite(exsorter->buffer, exsorter->item_size, exsorter->num_items, exsorter->fp_tmp); 
    if(num_wrote != exsorter->num_items) {
        tyn_log(stderr, "failed to write to exsorter temporary files");
        return TYN_ERROR;
    }
    exsorter->num_blocks++;
    exsorter->buffer_cur = exsorter->buffer;
    exsorter->num_items = 0;
}

int tyn_exsorter_sort(tyn_exsorter_t *exsorter) {
    size_t num_last_block_items = exsorter->num_items;
    if( num_last_block_items> 0) {
        tyn_exsorter_flush(exsorter);
    }
    size_t *num_block_items = (size_t *)malloc( sizeof(size_t) * exsorter->num_blocks);
    size_t *cur_block_items = (size_t *)malloc( sizeof(size_t) * exsorter->num_blocks);
    for( int i = 0; i < exsorter->num_blocks; i++ ) {
        *(num_block_items + i) = exsorter->max_num_items;
        *(cur_block_items + i) = 0;
    }
    if( num_last_block_items > 0) {
        *(num_block_items + exsorter->num_blocks - 1) = num_last_block_items;
    }
    
    //-----------------------------------------------------------------------
    //initialize memory buffer
    //here we use 3/4 buffer to store items, the rest 1/4 for write buffer
    
    printf("num items: %d\n", exsorter->max_num_items);
    printf("num blocks: %d\n", exsorter->num_blocks);
    
    size_t max_num_block_buffer_items = exsorter->buffer_size * 3 / 4 / exsorter->item_size / exsorter->num_blocks;
    size_t max_num_write_buffer_items = exsorter->buffer_size / 4 / exsorter->item_size;
    if(max_num_block_buffer_items == 0) {
        tyn_log(stderr, "buffer too small, try to increase");
        return TYN_ERROR;
    }
    size_t *num_block_buffer_items = (size_t *)malloc( sizeof(size_t) * exsorter->num_blocks);
    size_t *cur_block_buffer_items = (size_t *)malloc( sizeof(size_t) * exsorter->num_blocks);
    char **block_buffers = (char **)malloc( sizeof(char *) * exsorter->num_blocks);
    char **block_buffer_curs = (char **)malloc( sizeof(char *) * exsorter->num_blocks);
    char *write_buffer = exsorter->buffer + exsorter->num_blocks  * max_num_block_buffer_items * exsorter->item_size;
    char *write_buffer_cur = write_buffer;
    size_t num_write_buffer_items = 0;
    
    tyn_binary_heap_t * binary_heap = tyn_binary_heap_create(exsorter->num_blocks, exsorter->node_cmp_func);
    for( int i = 0; i < exsorter->num_blocks; i++ ) {
        *(block_buffers + i) = exsorter->buffer + i * max_num_block_buffer_items * exsorter->item_size;
        *(block_buffer_curs + i) = *(block_buffers + i);
        size_t num_items_to_read;
        if(*(num_block_items + i) < max_num_block_buffer_items) {
            num_items_to_read = *(num_block_items + i);
        } else {
            num_items_to_read = max_num_block_buffer_items;
        }
        *(num_block_buffer_items + i) = num_items_to_read;
        fseek(exsorter->fp_tmp, i * exsorter->max_num_items * exsorter->item_size, SEEK_SET);
        fread(*(block_buffers + i), exsorter->item_size, num_items_to_read, exsorter->fp_tmp);
        *(cur_block_items + i) = num_items_to_read;
        tyn_exsorter_node_t *node = (tyn_exsorter_node_t *)malloc( sizeof(tyn_exsorter_node_t) );
        void *item = (void *)malloc(exsorter->item_size);
        memcpy(item, *(block_buffer_curs + i), exsorter->item_size);
        *(block_buffer_curs + i) += exsorter->item_size;
        node->data = item;
        node->block_idx = i;
        tyn_binary_heap_insert(binary_heap, node);
        *(cur_block_buffer_items + i) = 1;
    }
    
    while(binary_heap->size) {
        tyn_exsorter_node_t *node = (tyn_exsorter_node_t *)tyn_binary_heap_extract_max(binary_heap);
        memcpy(write_buffer_cur, node->data, exsorter->item_size);
        write_buffer_cur += exsorter->item_size;
        num_write_buffer_items++;
        if(num_write_buffer_items == max_num_write_buffer_items) {
            fwrite(write_buffer, exsorter->item_size, num_write_buffer_items, exsorter->fp_write);
            write_buffer_cur = write_buffer;
            num_write_buffer_items = 0;
        }
        
        size_t block_idx = node->block_idx;
        if(*(cur_block_buffer_items + block_idx) < *(num_block_buffer_items + block_idx)) {
        
            //insert node to heap
            memcpy(node->data, *(block_buffer_curs + block_idx), exsorter->item_size);
            tyn_binary_heap_insert(binary_heap, node);
            *(block_buffer_curs + block_idx) += exsorter->item_size;
            (*(cur_block_buffer_items + block_idx))++;
            
            if(*(cur_block_buffer_items + block_idx) == *(num_block_buffer_items + block_idx) && *(cur_block_items + block_idx) < *(num_block_items + block_idx)) {
                size_t num_items_to_read;
                if(*(num_block_items + block_idx) - *(cur_block_items + block_idx) < max_num_block_buffer_items) {
                    num_items_to_read = *(num_block_items + block_idx) - *(cur_block_items + block_idx);
                } else {
                    num_items_to_read = max_num_block_buffer_items;
                }
                *(num_block_buffer_items + block_idx) = num_items_to_read;
                fseek(exsorter->fp_tmp, (block_idx * exsorter->max_num_items + *(cur_block_items + block_idx) )* exsorter->item_size, SEEK_SET);
                fread(*(block_buffers + block_idx), exsorter->item_size, num_items_to_read, exsorter->fp_tmp);
                *(block_buffer_curs + block_idx) = *(block_buffers + block_idx);
                *(cur_block_items + block_idx) += num_items_to_read;
                *(cur_block_buffer_items + block_idx) = 0;
            }
        } else {
                //the block is empty, free it
                free(node->data);
                free(node);
        }
    }
    //flush the write buffer
    if(num_write_buffer_items > 0) {
        fwrite(write_buffer, exsorter->item_size, num_write_buffer_items, exsorter->fp_write);
    }
    return 1;
    
    //--------------------------------------------------------------------------
    //initialize the heap
    /*
    tyn_binary_heap_t * binary_heap = tyn_binary_heap_create(exsorter->num_blocks, exsorter->node_cmp_func);
    for( int i = 0; i < exsorter->num_blocks; i++ ) {
        tyn_exsorter_node_t *node = (tyn_exsorter_node_t *)malloc( sizeof(tyn_exsorter_node_t) );
        fseek(exsorter->fp_tmp, i * exsorter->max_num_items * exsorter->item_size, SEEK_SET);
        void *item = (void *)malloc(exsorter->item_size);
        fread(item, 1, exsorter->item_size, exsorter->fp_tmp);
        node->data = item;
        node->block_idx = i;
        tyn_binary_heap_insert(binary_heap, node);
        (*(cur_block_items +i))++;
    }
    while(binary_heap->size) {
        tyn_exsorter_node_t *node = (tyn_exsorter_node_t *)tyn_binary_heap_extract_max(binary_heap);
        fwrite(node->data, 1, exsorter->item_size, exsorter->fp_write);
        if(*(cur_block_items + node->block_idx) < *(num_block_items + node->block_idx) ) {
            fseek(exsorter->fp_tmp, (node->block_idx * exsorter->max_num_items + *(cur_block_items + node->block_idx)) * exsorter->item_size, SEEK_SET);
            fread(node->data, 1, exsorter->item_size, exsorter->fp_tmp);
            //node->data = item;
            tyn_binary_heap_insert(binary_heap, node);
            (*(cur_block_items + node->block_idx))++;
        } else {
            free(node->data);
            free(node);
        }
    }
    free(num_block_items);
    free(cur_block_items);
    */
}


typedef struct test_item_t {
    int weight;
    size_t pos;
    size_t freq;
    char *s;
} test_item_t;

int cmp_test_item(test_item_t *a, test_item_t *b) {
    return a->weight - b->weight;
}

int node_cmp(tyn_exsorter_node_t *a, tyn_exsorter_node_t *b) {
    
    return ((test_item_t *)a->data)->weight < ((test_item_t *)b->data)->weight;
}
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
//gcc tyn_exsorter.c tyn_binary_heap.c tyn_utils.c -std=c99 -o test_exsorter -g
int main() {
    //srand ( time(NULL) );
    size_t buffer_size = 512*1024*1024;
    FILE *fp_tmp = fopen("exsorter_test.tmp", "w+b");
    FILE *fp_write = fopen("exsorter.dat", "w+b");
    if(!fp_write) {
        printf("null fp\n");
    }
    tyn_exsorter_t *exsorter = tyn_exsorter_create(buffer_size, sizeof(test_item_t), cmp_test_item, node_cmp, fp_tmp, fp_write);
    
    test_item_t *item = (test_item_t *)malloc(sizeof(test_item_t));
    
    item->pos = 2;
    item->freq = 3;
    int i;
    int count = 18134000;
    printf("file size should be %d x %d = %d\n", count, sizeof(test_item_t), count * sizeof(test_item_t));
    clock_t cl;
    cl = clock();
    for(int i = 0; i < count; i++) {
        item->weight = rand() % 1000000;
        tyn_exsorter_add(item, exsorter);
    }
    

    tyn_exsorter_sort(exsorter);
    cl = clock() - cl;
    float seconds = 1.0 * cl / CLOCKS_PER_SEC;
    printf("time took: %.3f sec\n", seconds);
    //verify result
    //seek(exsorter->fp_write, 0, SEEK_SET);
    rewind(exsorter->fp_write);
    fread(item, 1, sizeof(test_item_t), exsorter->fp_write);
    test_item_t *item_n = (test_item_t *)malloc(sizeof(test_item_t));
    
    while(!feof(exsorter->fp_write)) {
        fread(item_n, 1, sizeof(test_item_t), exsorter->fp_write);
        //printf("%d\n",item_n.weight);
        if(item_n->weight < item->weight) {
            printf("found error\n");
        }
        memcpy(item, item_n, sizeof(test_item_t));
    }
    
    
    
    //tyn_exsorter_sort(exsorter);
    tyn_exsorter_destroy(exsorter);
    fclose(fp_tmp);
    fclose(fp_write);
}



