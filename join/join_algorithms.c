#include <stdio.h>
#include <stdlib.h>
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
