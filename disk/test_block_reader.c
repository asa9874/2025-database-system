#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "block_reader.h"

void test_customer_reading() {
    printf("=== Customer 파일 블록 단위 읽기 테스트 ===\n\n");
    
    DiskReader *reader = disk_reader_open("../tbl/customer.tbl");
    if (!reader) {
        fprintf(stderr, "파일 열기 실패\n");
        return;
    }
    
    CustomerRecord record;
    long count = 0;
    clock_t start = clock();
    
    while (disk_reader_read_customer(reader, &record)) {
        count++;
        if (count <= 5) {
            printf("  Customer %ld: custkey=%ld, name=%s\n", 
                   count, record.custkey, record.name);
        }
    }
    
    clock_t end = clock();
    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;
    
    printf("\n총 %ld개 레코드 읽기 완료\n", count);
    printf("총 I/O 횟수: %ld\n", disk_reader_get_io_count(reader));
    printf("처리 시간: %.3f초\n", elapsed);
    printf("초당 처리: %.0f records/sec\n\n", count / elapsed);
    
    disk_reader_close(reader);
}

void test_order_reading() {
    printf("=== Orders 파일 블록 단위 읽기 테스트 ===\n\n");
    
    DiskReader *reader = disk_reader_open("../tbl/orders.tbl");
    if (!reader) {
        fprintf(stderr, "파일 열기 실패\n");
        return;
    }
    
    OrderRecord record;
    long count = 0;
    clock_t start = clock();
    
    while (disk_reader_read_order(reader, &record)) {
        count++;
        if (count <= 5) {
            printf("  Order %ld: orderkey=%ld, custkey=%ld, price=%.2f\n", 
                   count, record.orderkey, record.custkey, record.totalprice);
        }
    }
    
    clock_t end = clock();
    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;
    
    printf("\n총 %ld개 레코드 읽기 완료\n", count);
    printf("총 I/O 횟수: %ld\n", disk_reader_get_io_count(reader));
    printf("처리 시간: %.3f초\n", elapsed);
    printf("초당 처리: %.0f records/sec\n\n", count / elapsed);
    
    disk_reader_close(reader);
}

void test_reset_functionality() {
    printf("=== Reset 기능 테스트 ===\n\n");
    
    DiskReader *reader = disk_reader_open("../tbl/customer.tbl");
    if (!reader) {
        fprintf(stderr, "파일 열기 실패\n");
        return;
    }
    
    CustomerRecord record;
    
    printf("첫 번째 읽기:\n");
    for (int i = 0; i < 3; i++) {
        if (disk_reader_read_customer(reader, &record)) {
            printf("  Customer: custkey=%ld, name=%s\n", 
                   record.custkey, record.name);
        }
    }
    
    printf("\nReset 후 다시 읽기:\n");
    disk_reader_reset(reader);
    
    for (int i = 0; i < 3; i++) {
        if (disk_reader_read_customer(reader, &record)) {
            printf("  Customer: custkey=%ld, name=%s\n", 
                   record.custkey, record.name);
        }
    }
    
    disk_reader_close(reader);
    printf("\n");
}

int main() {
    printf("╔════════════════════════════════════════════════════════╗\n");
    printf("║       블록 단위 디스크 I/O 리더 테스트 프로그램       ║\n");
    printf("╚════════════════════════════════════════════════════════╝\n\n");
    
    test_customer_reading();
    test_order_reading();
    test_reset_functionality();
    
    printf("모든 테스트 완료!\n");
    return 0;
}
