#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "join_algorithms.h"
#include "disk_reader.h"
#include "disk_save.h"

// ========================================
// 결과를 디스크에 저장하는 Worker 함수
// ========================================

void* parallel_block_join_hash_worker_save(void* arg) {
    ThreadArg *thread_arg = (ThreadArg*)arg;
    long result_count = 0;
    
    DiskReader *cust_reader = disk_reader_open(thread_arg->customer_file, "customer");
    DiskReader *order_reader = disk_reader_open(thread_arg->order_file, "order");
    
    if (!cust_reader || !order_reader) {
        fprintf(stderr, "[Thread %d] 파일 열기 실패\n", thread_arg->thread_id);
        if (cust_reader) disk_reader_close(cust_reader);
        if (order_reader) disk_reader_close(order_reader);
        thread_arg->result_count = -1;
        return NULL;
    }
    
    int max_records = thread_arg->buffer_blocks * 200;
    CustomerRecord *cust_buffer = (CustomerRecord *)malloc(sizeof(CustomerRecord) * max_records);
    OrderRecord *order_buffer = (OrderRecord *)malloc(sizeof(OrderRecord) * max_records);
    
    if (!cust_buffer || !order_buffer) {
        fprintf(stderr, "[Thread %d] 버퍼 할당 실패\n", thread_arg->thread_id);
        free(cust_buffer);
        free(order_buffer);
        disk_reader_close(cust_reader);
        disk_reader_close(order_reader);
        thread_arg->result_count = -1;
        return NULL;
    }
    
    // 해시 테이블 생성
    HashNode **hash_table = (HashNode **)calloc(HASH_SIZE, sizeof(HashNode *));
    if (!hash_table) {
        fprintf(stderr, "[Thread %d] 해시 테이블 할당 실패\n", thread_arg->thread_id);
        free(cust_buffer);
        free(order_buffer);
        disk_reader_close(cust_reader);
        disk_reader_close(order_reader);
        thread_arg->result_count = -1;
        return NULL;
    }
    
    // 결과 버퍼 생성 (10000개씩 버퍼링)
    ResultBuffer *result_buf = result_buffer_create(thread_arg->output_file, 10000);
    if (!result_buf) {
        fprintf(stderr, "[Thread %d] 결과 버퍼 할당 실패\n", thread_arg->thread_id);
        free(hash_table);
        free(cust_buffer);
        free(order_buffer);
        disk_reader_close(cust_reader);
        disk_reader_close(order_reader);
        thread_arg->result_count = -1;
        return NULL;
    }
    
    // start_line까지 스킵
    CustomerRecord temp;
    for (long i = 0; i < thread_arg->start_line; i++) {
        if (!disk_reader_read_customer(cust_reader, &temp)) {
            break;
        }
    }
    
    long current_line = thread_arg->start_line;
    int cust_count;
    long initial_io;
    
    printf("[Thread %d] 시작: 라인 %ld ~ %ld (결과 저장 버전)\n", 
           thread_arg->thread_id, thread_arg->start_line, thread_arg->end_line);
    
    while (current_line < thread_arg->end_line) {
        cust_count = 0;
        initial_io = disk_reader_get_io_count();
        
        // Customer 블록 읽기
        while (cust_count < max_records && current_line < thread_arg->end_line) {
            if (!disk_reader_read_customer(cust_reader, &cust_buffer[cust_count])) {
                break;
            }
            cust_count++;
            current_line++;
            
            if (disk_reader_get_io_count() - initial_io >= thread_arg->buffer_blocks) {
                break;
            }
        }
        
        if (cust_count == 0) break;
        
        // 외부 블록으로 해시 테이블 구축
        for (int i = 0; i < cust_count; i++) {
            long key = cust_buffer[i].custkey;
            int hash = key % HASH_SIZE;
            
            HashNode *node = (HashNode *)malloc(sizeof(HashNode));
            node->custkey = key;
            node->customer_idx = i;
            node->next = hash_table[hash];
            hash_table[hash] = node;
        }
        
        disk_reader_reset(order_reader);
        
        // Orders 전체 스캔
        int order_count;
        while (1) {
            order_count = 0;
            initial_io = disk_reader_get_io_count();
            
            while (order_count < max_records) {
                if (!disk_reader_read_order(order_reader, &order_buffer[order_count])) {
                    break;
                }
                order_count++;
                
                if (disk_reader_get_io_count() - initial_io >= thread_arg->buffer_blocks) {
                    break;
                }
            }
            
            if (order_count == 0) break;
            
            // 해시 테이블 탐색 및 결과 저장
            for (int j = 0; j < order_count; j++) {
                long key = order_buffer[j].custkey;
                int hash = key % HASH_SIZE;
                
                HashNode *node = hash_table[hash];
                while (node) {
                    if (node->custkey == key) {
                        // 매칭된 결과를 버퍼에 추가
                        result_buffer_add(result_buf, 
                                        &cust_buffer[node->customer_idx], 
                                        &order_buffer[j]);
                        result_count++;
                    }
                    node = node->next;
                }
            }
        }
        
        // 해시 테이블 정리
        for (int i = 0; i < HASH_SIZE; i++) {
            HashNode *node = hash_table[i];
            while (node) {
                HashNode *temp = node;
                node = node->next;
                free(temp);
            }
            hash_table[i] = NULL;
        }
    }
    
    printf("[Thread %d] 완료: %ld건 매칭 및 저장\n", thread_arg->thread_id, result_count);
    
    // 남은 결과 플러시 및 정리
    result_buffer_destroy(result_buf);
    free(hash_table);
    free(cust_buffer);
    free(order_buffer);
    disk_reader_close(cust_reader);
    disk_reader_close(order_reader);
    
    thread_arg->result_count = result_count;
    return NULL;
}

long disk_parallel_block_nested_loop_join_hash_save(const char *customer_file, const char *order_file, 
                                                     int buffer_blocks, const char *output_file, int num_threads) {
    // 출력 파일 초기화
    if (disk_save_init(output_file) != 0) {
        fprintf(stderr, "출력 파일 초기화 실패\n");
        return -1;
    }
    
    // Customer 파일의 총 라인 수 계산
    FILE *fp = fopen(customer_file, "r");
    if (!fp) {
        perror("fopen");
        return -1;
    }
    
    long total_lines = 0;
    char ch;
    while ((ch = fgetc(fp)) != EOF) {
        if (ch == '\n') total_lines++;
    }
    fclose(fp);
    
    printf("총 Customer 레코드: %ld개\n", total_lines);
    printf("%d개 스레드로 병렬 처리 시작 (결과 저장 모드)...\n", num_threads);
    printf("출력 파일: %s\n\n", output_file);
    
    // 각 스레드의 작업 범위 계산
    long chunk_size = total_lines / num_threads;
    
    pthread_t *threads = (pthread_t *)malloc(sizeof(pthread_t) * num_threads);
    ThreadArg *thread_args = (ThreadArg *)malloc(sizeof(ThreadArg) * num_threads);
    
    if (!threads || !thread_args) {
        fprintf(stderr, "스레드 배열 할당 실패\n");
        free(threads);
        free(thread_args);
        return -1;
    }
    
    for (int i = 0; i < num_threads; i++) {
        thread_args[i].customer_file = customer_file;
        thread_args[i].order_file = order_file;
        thread_args[i].buffer_blocks = buffer_blocks;
        thread_args[i].start_line = i * chunk_size;
        thread_args[i].end_line = (i == num_threads - 1) ? total_lines : (i + 1) * chunk_size;
        thread_args[i].result_count = 0;
        thread_args[i].thread_id = i + 1;
        thread_args[i].output_file = output_file;
        
        if (pthread_create(&threads[i], NULL, parallel_block_join_hash_worker_save, &thread_args[i]) != 0) {
            fprintf(stderr, "Thread %d 생성 실패\n", i + 1);
            free(threads);
            free(thread_args);
            return -1;
        }
    }
    
    // 모든 스레드 완료 대기
    long total_result = 0;
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
        if (thread_args[i].result_count >= 0) {
            total_result += thread_args[i].result_count;
        }
    }
    
    // 메모리 해제
    free(threads);
    free(thread_args);
    
    // 출력 파일 마무리 (통계 정보 추가)
    disk_save_finalize(output_file, total_result);
    
    printf("\n병렬 처리 완료 (결과 저장 버전)!\n");
    return total_result;
}
