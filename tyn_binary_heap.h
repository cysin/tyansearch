

typedef struct tyn_binary_heap_t {
  size_t size;
  size_t max_size;
  void **nodes;
  int (*cmp)(const void *a, const void *b);
} tyn_binary_heap_t;