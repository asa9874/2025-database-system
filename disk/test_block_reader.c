#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "block_reader.h"

void test_customer_reading() {
    printf("=== Customer 파일 블록 단위 읽기 테스트 ===\n\n");
    
    BlockReader *reader = block_reader_open("../tbl/customer.tbl");
    if (!reader) {
        fprintf(stderr, "파일 열기 실패\n");
        return;
    }
    
    CustomerRecord record;
    long count = 0;
    clock_t start = clock();
    
    while (block_reader_read_customer(reader, &record)) {
        count++;
        if (count <= 5) {
            printf("  Customer %ld: custkey=%ld, name=%s\n", 
                   count, record.custkey, record.name);
        }
    }
    
    clock_t end = clock();
    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;
    
    printf("\n총 %ld개 레코드 읽기 완료\n", count);
    printf("총 I/O 횟수: %ld\n", block_reader_get_io_count(reader));
    printf("처리 시간: %.3f초\n", elapsed);
    printf("초당 처리: %.0f records/sec\n\n", count / elapsed);
    
    block_reader_close(reader);
}

void test_order_reading() {
    printf("=== Orders 파일 블록 단위 읽기 테스트 ===\n\n");
    
    BlockReader *reader = block_reader_open("../tbl/orders.tbl");
    if (!reader) {
        fprintf(stderr, "파일 열기 실패\n");
        return;
    }
    
    OrderRecord record;
    long count = 0;
    clock_t start = clock();
    
    while (block_reader_read_order(reader, &record)) {
        count++;
        if (count <= 5) {
            printf("  Order %ld: orderkey=%ld, custkey=%ld, price=%.2f\n", 
                   count, record.orderkey, record.custkey, record.totalprice);
        }
    }
    
    clock_t end = clock();
    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;
    
    printf("\n총 %ld개 레코드 읽기 완료\n", count);
    printf("총 I/O 횟수: %ld\n", block_reader_get_io_count(reader));
    printf("처리 시간: %.3f초\n", elapsed);
    printf("초당 처리: %.0f records/sec\n\n", count / elapsed);
    
    block_reader_close(reader);
}

void test_reset_functionality() {
    printf("=== Reset 기능 테스트 ===\n\n");
    
    BlockReader *reader = block_reader_open("../tbl/customer.tbl");
    if (!reader) {
        fprintf(stderr, "파일 열기 실패\n");
        return;
    }
    
    CustomerRecord record;
    
    printf("첫 번째 읽기:\n");
    for (int i = 0; i < 3; i++) {
        if (block_reader_read_customer(reader, &record)) {
            printf("  Customer: custkey=%ld, name=%s\n", 
                   record.custkey, record.name);
        }
    }
    
    printf("\nReset 후 다시 읽기:\n");
    block_reader_reset(reader);
    
    for (int i = 0; i < 3; i++) {
        if (block_reader_read_customer(reader, &record)) {
            printf("  Customer: custkey=%ld, name=%s\n", 
                   record.custkey, record.name);
        }
    }
    
    block_reader_close(reader);
    printf("\n");
}

void test_block_efficiency() {
    printf("=== 블록 효율성 비교 테스트 ===\n\n");
    
    // 블록 단위 읽기
    block_reader_reset_global_io_count();
    BlockReader *reader = block_reader_open("../tbl/customer.tbl");
    if (!reader) {
        fprintf(stderr, "파일 열기 실패\n");
        return;
    }
    
    CustomerRecord record;
    long count = 0;
    clock_t start = clock();
    
    while (block_reader_read_customer(reader, &record)) {
        count++;
    }
    
    clock_t end = clock();
    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;
    long io_count = block_reader_get_io_count(reader);
    
    printf("블록 단위 읽기 (BLOCK_SIZE=%d):\n", BLOCK_SIZE);
    printf("  레코드 수: %ld\n", count);
    printf("  I/O 횟수: %ld\n", io_count);
    printf("  처리 시간: %.3f초\n", elapsed);
    printf("  블록당 평균 레코드: %.1f\n", (double)count / io_count);
    printf("  I/O 효율: %.2f%%\n", 100.0 * io_count / count);
    
    block_reader_close(reader);
}

void test_simple_join() {
    printf("=== 간단한 조인 테스트 (첫 100개 Customer) ===\n\n");
    
    BlockReader *cust_reader = block_reader_open("../tbl/customer.tbl");
    BlockReader *order_reader = block_reader_open("../tbl/orders.tbl");
    
    if (!cust_reader || !order_reader) {
        fprintf(stderr, "파일 열기 실패\n");
        if (cust_reader) block_reader_close(cust_reader);
        if (order_reader) block_reader_close(order_reader);
        return;
    }
    
    CustomerRecord cust;
    OrderRecord ord;
    long join_count = 0;
    long cust_count = 0;
    
    clock_t start = clock();
    
    while (block_reader_read_customer(cust_reader, &cust) && cust_count < 100) {
        cust_count++;
        
        block_reader_reset(order_reader);
        while (block_reader_read_order(order_reader, &ord)) {
            if (cust.custkey == ord.custkey) {
                join_count++;
            }
        }
    }
    
    clock_t end = clock();
    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;
    
    printf("Customer %ld개 처리 완료\n", cust_count);
    printf("매칭된 레코드: %ld\n", join_count);
    printf("총 I/O 횟수: %ld (Customer) + %ld (Orders)\n", 
           block_reader_get_io_count(cust_reader),
           block_reader_get_io_count(order_reader));
    printf("처리 시간: %.3f초\n\n", elapsed);
    
    block_reader_close(cust_reader);
    block_reader_close(order_reader);
}

int main() {
    printf("╔════════════════════════════════════════════════════════╗\n");
    printf("║       블록 단위 디스크 I/O 리더 테스트 프로그램       ║\n");
    printf("╚════════════════════════════════════════════════════════╝\n\n");
    
    test_customer_reading();
    test_order_reading();
    test_reset_functionality();
    test_block_efficiency();
    test_simple_join();
    
    printf("모든 테스트 완료!\n");
    return 0;
}
