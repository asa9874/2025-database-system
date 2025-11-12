#ifndef JOIN_ALGORITHMS_H
#define JOIN_ALGORITHMS_H

typedef struct HashNode {
    long custkey;
    int customer_idx;
    struct HashNode *next;
} HashNode;

#define HASH_SIZE 100003

long nested_loop_join(int show_progress);
long hash_join(void);

#endif
