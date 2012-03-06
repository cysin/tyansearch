

typedef struct tyn_exsorter_node_t {
    void *data;
    size_t block_idx;
} tyn_exsorter_node_t;

typedef struct tyn_exsorter_t {
    char *buffer;
    size_t buffer_size;
    char *buffer_cur;
    size_t item_size;
    size_t max_num_items;
    size_t num_items;
    size_t num_blocks;
    int (* cmp_func)(void *, void*);
    int (* node_cmp_func)(tyn_exsorter_node_t *, tyn_exsorter_node_t *);
    FILE *fp_tmp;
    FILE *fp_write;

} tyn_exsorter_t;