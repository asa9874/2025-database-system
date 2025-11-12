#ifndef JOIN_ALGORITHMS_H
#define JOIN_ALGORITHMS_H

typedef struct HashNode {
    long custkey;
    int customer_idx;
    struct HashNode *next;
} HashNode;

#define HASH_SIZE 100003

long disk_nested_loop_join(const char *customer_file, const char *order_file, int show_progress);
long disk_block_nested_loop_join(const char *customer_file, const char *order_file, int buffer_blocks);
long disk_hash_join(const char *customer_file, const char *order_file);

#endif
