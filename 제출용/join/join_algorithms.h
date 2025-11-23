#ifndef JOIN_ALGORITHMS_H
#define JOIN_ALGORITHMS_H

#include <pthread.h>

typedef struct HashNode {
    long custkey;
    int customer_idx;
    struct HashNode *next;
} HashNode;

#define HASH_SIZE 100003

typedef struct {
    const char *customer_file;
    const char *order_file;
    int block_size;
    long start_line;
    long end_line;
    long result_count;
    int thread_id;
    const char *output_file;  // 결과 저장을 위한 출력 파일 경로
} ThreadArg;

// 결과를 디스크에 저장하는 버전 (유일하게 사용되는 함수)
long disk_parallel_block_nested_loop_join_hash_save(const char *customer_file, const char *order_file, 
                                                     int block_size, const char *output_file, int num_threads);

#endif
