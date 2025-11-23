#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "disk_reader.h"

void test_customer_reading() {
    printf("=== Customer 파일 라인 단위 읽기 테스트 (기존 방식) ===\n\n");
    
    DiskReader *reader = disk_reader_open("../tbl/customer.tbl", "customer");
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
            printf("  Customer %ld:\n", count);
            printf("    custkey=%ld\n", record.custkey);
            printf("    name=%s\n", record.name);
            printf("    address=%s\n", record.address);
            printf("    nationkey=%ld\n", record.nationkey);
            printf("    phone=%s\n", record.phone);
            printf("    acctbal=%.2f\n", record.acctbal);
            printf("    mktsegment=%s\n", record.mktsegment);
            printf("\n");
        }
    }
    
    clock_t end = clock();
    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;
    
    printf("\n총 %ld개 레코드 읽기 완료\n", count);
    printf("총 I/O 횟수: %ld\n", disk_reader_get_io_count());
    printf("처리 시간: %.3f초\n", elapsed);
    printf("초당 처리: %.0f records/sec\n\n", count / elapsed);
    
    disk_reader_close(reader);
}

void test_order_reading() {
    printf("=== Orders 파일 라인 단위 읽기 테스트 (기존 방식) ===\n\n");
    
    DiskReader *reader = disk_reader_open("../tbl/orders.tbl", "order");
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
            printf("  Order %ld:\n", count);
            printf("    orderkey=%ld\n", record.orderkey);
            printf("    custkey=%ld\n", record.custkey);
            printf("    orderstatus=%c\n", record.orderstatus);
            printf("    totalprice=%.2f\n", record.totalprice);
            printf("    orderdate=%s\n", record.orderdate);
            printf("    orderpriority=%s\n", record.orderpriority);
            printf("    clerk=%s\n", record.clerk);
            printf("    shippriority=%ld\n", record.shippriority);
            printf("\n");
        }
    }
    
    clock_t end = clock();
    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;
    
    printf("\n총 %ld개 레코드 읽기 완료\n", count);
    printf("총 I/O 횟수: %ld\n", disk_reader_get_io_count());
    printf("처리 시간: %.3f초\n", elapsed);
    printf("초당 처리: %.0f records/sec\n\n", count / elapsed);
    
    disk_reader_close(reader);
}

void test_reset_functionality() {
    printf("=== Reset 기능 테스트 ===\n\n");
    
    DiskReader *reader = disk_reader_open("../tbl/customer.tbl", "customer");
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

void test_io_efficiency() {
    printf("=== I/O 효율성 테스트 (라인 단위 읽기) ===\n\n");
    
    // I/O 카운터 리셋
    disk_reader_reset_io_count();
    
    DiskReader *reader = disk_reader_open("../tbl/customer.tbl", "customer");
    if (!reader) {
        fprintf(stderr, "파일 열기 실패\n");
        return;
    }
    
    CustomerRecord record;
    long count = 0;
    clock_t start = clock();
    
    while (disk_reader_read_customer(reader, &record)) {
        count++;
    }
    
    clock_t end = clock();
    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;
    long io_count = disk_reader_get_io_count();
    
    printf("라인 단위 읽기 (fgets 방식):\n");
    printf("  레코드 수: %ld\n", count);
    printf("  I/O 횟수: %ld\n", io_count);
    printf("  처리 시간: %.3f초\n", elapsed);
    printf("  블록당 평균 레코드: %.1f\n", (double)count / io_count);
    printf("  I/O 효율: %.2f%%\n\n", 100.0 * io_count / count);
    
    disk_reader_close(reader);
}

int main() {
    printf("╔════════════════════════════════════════════════════════╗\n");
    printf("║     라인 단위 디스크 I/O 리더 테스트 프로그램         ║\n");
    printf("║              (기존 disk_reader.c)                     ║\n");
    printf("╚════════════════════════════════════════════════════════╝\n\n");
    
    test_customer_reading();
    test_order_reading();
    test_reset_functionality();
    test_io_efficiency();
    
    printf("모든 테스트 완료!\n");
    return 0;
}
