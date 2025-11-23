#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "block_reader.h"

void print_separator() {
    printf("================================================================\n");
}

void test_performance_customer() {
    printf("Customer 파일 성능 테스트 (BLOCK_SIZE = %d bytes)\n", BLOCK_SIZE);
    print_separator();
    
    disk_reader_reset_global_io_count();
    
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
    }
    
    clock_t end = clock();
    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;
    long io_count = disk_reader_get_io_count(reader);
    
    printf("총 레코드 수:      %10ld 개\n", count);
    printf("총 I/O 횟수:       %10ld 회\n", io_count);
    printf("블록당 레코드:     %10.1f 개\n", (double)count / io_count);
    printf("I/O 효율:          %10.2f %%\n", 100.0 * io_count / count);
    printf("처리 시간:         %10.3f 초\n", elapsed);
    printf("초당 처리:         %10.0f records/sec\n\n", count / elapsed);
    
    disk_reader_close(reader);
}

void test_performance_orders() {
    printf("Orders 파일 성능 테스트 (BLOCK_SIZE = %d bytes)\n", BLOCK_SIZE);
    print_separator();
    
    disk_reader_reset_global_io_count();
    
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
    }
    
    clock_t end = clock();
    double elapsed = (double)(end - start) / CLOCKS_PER_SEC;
    long io_count = disk_reader_get_io_count(reader);
    
    printf("총 레코드 수:      %10ld 개\n", count);
    printf("총 I/O 횟수:       %10ld 회\n", io_count);
    printf("블록당 레코드:     %10.1f 개\n", (double)count / io_count);
    printf("I/O 효율:          %10.2f %%\n", 100.0 * io_count / count);
    printf("처리 시간:         %10.3f 초\n", elapsed);
    printf("초당 처리:         %10.0f records/sec\n\n", count / elapsed);
    
    disk_reader_close(reader);
}

int main() {
    printf("\n");
    printf("===============================================================\n");
    printf("           블록 크기별 성능 비교 테스트 프로그램              \n");
    printf("===============================================================\n\n");
    
    printf("현재 설정:\n");
    printf("  BLOCK_SIZE:     %d bytes (%d KB)\n", BLOCK_SIZE, BLOCK_SIZE / 1024);
    printf("  MAX_LINE_SIZE:  %d bytes\n\n", MAX_LINE_SIZE);
    
    test_performance_customer();
    test_performance_orders();
    
    printf("모든 테스트 완료!\n\n");
    printf("다른 블록 크기로 테스트하려면:\n");
    printf("  1. block_reader.h에서 BLOCK_SIZE 값 변경\n");
    printf("  2. make clean && make\n");
    printf("  3. make run_block_size 또는 make benchmark\n\n");
    
    return 0;
}
