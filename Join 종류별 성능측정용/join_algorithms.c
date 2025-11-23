#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "join_algorithms.h"
#include "disk_reader.h"

long disk_nested_loop_join(const char *customer_file, const char *order_file, int show_progress) {
    long result_count = 0;
    DiskReader *cust_reader = disk_reader_open(customer_file, "customer");
    DiskReader *order_reader = disk_reader_open(order_file, "order");
    
    if (!cust_reader || !order_reader) {
        fprintf(stderr, "파일 열기 실패\n");
        if (cust_reader) disk_reader_close(cust_reader);
        if (order_reader) disk_reader_close(order_reader);
        return -1;
    }
    
    CustomerRecord cust;
    OrderRecord ord;
    long cust_count = 0;
    
    while (disk_reader_read_customer(cust_reader, &cust)) {
        cust_count++;
        if (show_progress && cust_count % 1000 == 0) {
            printf("\r  처리 중... Customer: %ld", cust_count);
            fflush(stdout);
        }
        
        disk_reader_reset(order_reader);
        while (disk_reader_read_order(order_reader, &ord)) {
            if (cust.custkey == ord.custkey) {
                result_count++;
            }
        }
    }
    
    if (show_progress) {
        printf("\r  처리 완료: Customer %ld개 처리                \n", cust_count);
    }
    
    disk_reader_close(cust_reader);
    disk_reader_close(order_reader);
    
    return result_count;
}

long disk_block_nested_loop_join(const char *customer_file, const char *order_file, int buffer_blocks) {
    long result_count = 0;
    DiskReader *cust_reader = disk_reader_open(customer_file, "customer");
    DiskReader *order_reader = disk_reader_open(order_file, "order");
    
    if (!cust_reader || !order_reader) {
        fprintf(stderr, "파일 열기 실패\n");
        if (cust_reader) disk_reader_close(cust_reader);
        if (order_reader) disk_reader_close(order_reader);
        return -1;
    }
    
    int max_records = buffer_blocks * 200;
    CustomerRecord *cust_buffer = (CustomerRecord *)malloc(sizeof(CustomerRecord) * max_records);
    OrderRecord *order_buffer = (OrderRecord *)malloc(sizeof(OrderRecord) * max_records);
    
    if (!cust_buffer || !order_buffer) {
        fprintf(stderr, "버퍼 할당 실패\n");
        free(cust_buffer);
        free(order_buffer);
        disk_reader_close(cust_reader);
        disk_reader_close(order_reader);
        return -1;
    }
    
    int cust_count;
    long initial_io;
    int iteration = 0;
    
    while (1) {
        cust_count = 0;
        initial_io = disk_reader_get_io_count();
        
        while (cust_count < max_records) {
            if (!disk_reader_read_customer(cust_reader, &cust_buffer[cust_count])) {
                break;
            }
            cust_count++;
            
            if (disk_reader_get_io_count() - initial_io >= buffer_blocks) {
                break;
            }
        }
        
        if (cust_count == 0) break;
        
        iteration++;
        printf("\r  외부 테이블 블록 %d 처리 중 (레코드 %d개)...", iteration, cust_count);
        fflush(stdout);
        
        disk_reader_reset(order_reader);
        
        int order_count;
        while (1) {
            order_count = 0;
            initial_io = disk_reader_get_io_count();
            
            while (order_count < max_records) {
                if (!disk_reader_read_order(order_reader, &order_buffer[order_count])) {
                    break;
                }
                order_count++;
                
                if (disk_reader_get_io_count() - initial_io >= buffer_blocks) {
                    break;
                }
            }
            
            if (order_count == 0) break;
            
            for (int i = 0; i < cust_count; i++) {
                for (int j = 0; j < order_count; j++) {
                    if (cust_buffer[i].custkey == order_buffer[j].custkey) {
                        result_count++;
                    }
                }
            }
        }
    }
    
    printf("\r  처리 완료: 외부 테이블 %d 블록 처리                    \n", iteration);
    
    free(cust_buffer);
    free(order_buffer);
    disk_reader_close(cust_reader);
    disk_reader_close(order_reader);
    
    return result_count;
}

long disk_block_nested_loop_join_hash(const char *customer_file, const char *order_file, int buffer_blocks) {
    long result_count = 0;
    DiskReader *cust_reader = disk_reader_open(customer_file, "customer");
    DiskReader *order_reader = disk_reader_open(order_file, "order");
    
    if (!cust_reader || !order_reader) {
        fprintf(stderr, "파일 열기 실패\n");
        if (cust_reader) disk_reader_close(cust_reader);
        if (order_reader) disk_reader_close(order_reader);
        return -1;
    }
    
    int max_records = buffer_blocks * 200;
    CustomerRecord *cust_buffer = (CustomerRecord *)malloc(sizeof(CustomerRecord) * max_records);
    OrderRecord *order_buffer = (OrderRecord *)malloc(sizeof(OrderRecord) * max_records);
    
    if (!cust_buffer || !order_buffer) {
        fprintf(stderr, "버퍼 할당 실패\n");
        free(cust_buffer);
        free(order_buffer);
        disk_reader_close(cust_reader);
        disk_reader_close(order_reader);
        return -1;
    }
    
    // 해시 테이블 생성 (블록별로 재사용)
    HashNode **hash_table = (HashNode **)calloc(HASH_SIZE, sizeof(HashNode *));
    if (!hash_table) {
        fprintf(stderr, "해시 테이블 할당 실패\n");
        free(cust_buffer);
        free(order_buffer);
        disk_reader_close(cust_reader);
        disk_reader_close(order_reader);
        return -1;
    }
    
    int cust_count;
    long initial_io;
    int iteration = 0;
    
    while (1) {
        cust_count = 0;
        initial_io = disk_reader_get_io_count();
        
        // 외부 블록(Customer) 읽기
        while (cust_count < max_records) {
            if (!disk_reader_read_customer(cust_reader, &cust_buffer[cust_count])) {
                break;
            }
            cust_count++;
            
            if (disk_reader_get_io_count() - initial_io >= buffer_blocks) {
                break;
            }
        }
        
        if (cust_count == 0) break;
        
        iteration++;
        printf("\r  외부 테이블 블록 %d 처리 중 (레코드 %d개)...", iteration, cust_count);
        fflush(stdout);
        
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
        
        // 내부 블록(Orders) 처리
        int order_count;
        while (1) {
            order_count = 0;
            initial_io = disk_reader_get_io_count();
            
            while (order_count < max_records) {
                if (!disk_reader_read_order(order_reader, &order_buffer[order_count])) {
                    break;
                }
                order_count++;
                
                if (disk_reader_get_io_count() - initial_io >= buffer_blocks) {
                    break;
                }
            }
            
            if (order_count == 0) break;
            
            // 내부 블록으로 해시 테이블 탐색 (Probe)
            for (int j = 0; j < order_count; j++) {
                long key = order_buffer[j].custkey;
                int hash = key % HASH_SIZE;
                
                HashNode *node = hash_table[hash];
                while (node) {
                    if (node->custkey == key) {
                        result_count++;
                    }
                    node = node->next;
                }
            }
        }
        
        // 해시 테이블 정리 (다음 외부 블록을 위해)
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
    
    printf("\r  처리 완료: 외부 테이블 %d 블록 처리                    \n", iteration);
    
    free(hash_table);
    free(cust_buffer);
    free(order_buffer);
    disk_reader_close(cust_reader);
    disk_reader_close(order_reader);
    
    return result_count;
}

void* parallel_block_join_worker(void* arg) {
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
    
    printf("[Thread %d] 시작: 라인 %ld ~ %ld\n", 
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
            
            // 조인 수행
            for (int i = 0; i < cust_count; i++) {
                for (int j = 0; j < order_count; j++) {
                    if (cust_buffer[i].custkey == order_buffer[j].custkey) {
                        result_count++;
                    }
                }
            }
        }
    }
    
    printf("[Thread %d] 완료: %ld건 매칭\n", thread_arg->thread_id, result_count);
    
    free(cust_buffer);
    free(order_buffer);
    disk_reader_close(cust_reader);
    disk_reader_close(order_reader);
    
    thread_arg->result_count = result_count;
    return NULL;
}

long disk_parallel_block_nested_loop_join(const char *customer_file, const char *order_file, int buffer_blocks) {
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
    printf("4개 스레드로 병렬 처리 시작...\n\n");
    
    // 각 스레드의 작업 범위 계산
    long chunk_size = total_lines / NUM_THREADS;
    
    pthread_t threads[NUM_THREADS];
    ThreadArg thread_args[NUM_THREADS];
    
    for (int i = 0; i < NUM_THREADS; i++) {
        thread_args[i].customer_file = customer_file;
        thread_args[i].order_file = order_file;
        thread_args[i].buffer_blocks = buffer_blocks;
        thread_args[i].start_line = i * chunk_size;
        thread_args[i].end_line = (i == NUM_THREADS - 1) ? total_lines : (i + 1) * chunk_size;
        thread_args[i].result_count = 0;
        thread_args[i].thread_id = i + 1;
        
        if (pthread_create(&threads[i], NULL, parallel_block_join_worker, &thread_args[i]) != 0) {
            fprintf(stderr, "Thread %d 생성 실패\n", i + 1);
            return -1;
        }
    }
    
    // 모든 스레드 완료 대기
    long total_result = 0;
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
        if (thread_args[i].result_count >= 0) {
            total_result += thread_args[i].result_count;
        }
    }
    
    printf("\n병렬 처리 완료!\n");
    return total_result;
}

void* parallel_block_join_hash_worker(void* arg) {
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
    
    printf("[Thread %d] 시작: 라인 %ld ~ %ld (해시 버전)\n", 
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
            
            // 해시 테이블 탐색
            for (int j = 0; j < order_count; j++) {
                long key = order_buffer[j].custkey;
                int hash = key % HASH_SIZE;
                
                HashNode *node = hash_table[hash];
                while (node) {
                    if (node->custkey == key) {
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
    
    printf("[Thread %d] 완료: %ld건 매칭\n", thread_arg->thread_id, result_count);
    
    free(hash_table);
    free(cust_buffer);
    free(order_buffer);
    disk_reader_close(cust_reader);
    disk_reader_close(order_reader);
    
    thread_arg->result_count = result_count;
    return NULL;
}

long disk_parallel_block_nested_loop_join_hash(const char *customer_file, const char *order_file, int buffer_blocks) {
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
    printf("4개 스레드로 병렬 처리 시작 (블록별 해시 적용)...\n\n");
    
    // 각 스레드의 작업 범위 계산
    long chunk_size = total_lines / NUM_THREADS;
    
    pthread_t threads[NUM_THREADS];
    ThreadArg thread_args[NUM_THREADS];
    
    for (int i = 0; i < NUM_THREADS; i++) {
        thread_args[i].customer_file = customer_file;
        thread_args[i].order_file = order_file;
        thread_args[i].buffer_blocks = buffer_blocks;
        thread_args[i].start_line = i * chunk_size;
        thread_args[i].end_line = (i == NUM_THREADS - 1) ? total_lines : (i + 1) * chunk_size;
        thread_args[i].result_count = 0;
        thread_args[i].thread_id = i + 1;
        
        if (pthread_create(&threads[i], NULL, parallel_block_join_hash_worker, &thread_args[i]) != 0) {
            fprintf(stderr, "Thread %d 생성 실패\n", i + 1);
            return -1;
        }
    }
    
    // 모든 스레드 완료 대기
    long total_result = 0;
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
        if (thread_args[i].result_count >= 0) {
            total_result += thread_args[i].result_count;
        }
    }
    
    printf("\n병렬 처리 완료 (해시 버전)!\n");
    return total_result;
}

long disk_hash_join(const char *customer_file, const char *order_file) {
    long result_count = 0;
    DiskReader *cust_reader = disk_reader_open(customer_file, "customer");
    DiskReader *order_reader = disk_reader_open(order_file, "order");
    
    if (!cust_reader || !order_reader) {
        fprintf(stderr, "파일 열기 실패\n");
        if (cust_reader) disk_reader_close(cust_reader);
        if (order_reader) disk_reader_close(order_reader);
        return -1;
    }
    
    HashNode **hash_table = (HashNode **)calloc(HASH_SIZE, sizeof(HashNode *));
    if (!hash_table) {
        fprintf(stderr, "해시 테이블 할당 실패\n");
        disk_reader_close(cust_reader);
        disk_reader_close(order_reader);
        return -1;
    }
    
    printf("  Build Phase: 해시 테이블 구축 중...\n");
    CustomerRecord cust;
    long cust_count = 0;
    while (disk_reader_read_customer(cust_reader, &cust)) {
        cust_count++;
        if (cust_count % 10000 == 0) {
            printf("\r    Customer %ld개 처리...", cust_count);
            fflush(stdout);
        }
        
        long key = cust.custkey;
        int hash = key % HASH_SIZE;
        
        HashNode *node = (HashNode *)malloc(sizeof(HashNode));
        node->custkey = key;
        node->customer_idx = 0;
        node->next = hash_table[hash];
        hash_table[hash] = node;
    }
    printf("\r    Build 완료: Customer %ld개 처리      \n", cust_count);
    
    printf("  Probe Phase: Orders 테이블 스캔 중...\n");
    OrderRecord ord;
    long order_count = 0;
    while (disk_reader_read_order(order_reader, &ord)) {
        order_count++;
        if (order_count % 100000 == 0) {
            printf("\r    Order %ld개 처리...", order_count);
            fflush(stdout);
        }
        
        long key = ord.custkey;
        int hash = key % HASH_SIZE;
        
        HashNode *node = hash_table[hash];
        while (node) {
            if (node->custkey == key) {
                result_count++;
            }
            node = node->next;
        }
    }
    printf("\r    Probe 완료: Order %ld개 처리      \n", order_count);
    
    for (int i = 0; i < HASH_SIZE; i++) {
        HashNode *node = hash_table[i];
        while (node) {
            HashNode *temp = node;
            node = node->next;
            free(temp);
        }
    }
    free(hash_table);
    
    disk_reader_close(cust_reader);
    disk_reader_close(order_reader);
    
    return result_count;
}
