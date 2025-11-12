#include <stdio.h>
#include <stdlib.h>
#include "data_loader.h"
#include "join_algorithms.h"

long nested_loop_join(int show_progress) {
    long result_count = 0;
    
    for (int i = 0; i < num_customers; i++) {
        for (int j = 0; j < num_orders; j++) {
            if (customers[i].custkey == orders[j].custkey) {
                result_count++;
            }
        }
    }
    
    return result_count;
}

long hash_join(void) {
    long result_count = 0;
    
    HashNode **hash_table = (HashNode **)calloc(HASH_SIZE, sizeof(HashNode *));
    if (!hash_table) {
        fprintf(stderr, "해시 테이블 할당 실패\n");
        return -1;
    }
    
    for (int i = 0; i < num_customers; i++) {
        long key = customers[i].custkey;
        int hash = key % HASH_SIZE;
        
        HashNode *node = (HashNode *)malloc(sizeof(HashNode));
        node->custkey = key;
        node->customer_idx = i;
        node->next = hash_table[hash];
        hash_table[hash] = node;
    }
    
    for (int j = 0; j < num_orders; j++) {
        long key = orders[j].custkey;
        int hash = key % HASH_SIZE;
        
        HashNode *node = hash_table[hash];
        while (node) {
            if (node->custkey == key) {
                result_count++;
            }
            node = node->next;
        }
    }
    
    for (int i = 0; i < HASH_SIZE; i++) {
        HashNode *node = hash_table[i];
        while (node) {
            HashNode *temp = node;
            node = node->next;
            free(temp);
        }
    }
    free(hash_table);
    
    return result_count;
}
