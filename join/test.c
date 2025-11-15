#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include "join_algorithms.h"
#include "disk_reader.h"

double get_time_sec() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

void run_disk_nested_loop_join_test(const char *customer_file, const char *order_file) {
    printf("[테스트 1: 중첩 루프 조인 (Nested Loop Join)]\n");
    
    disk_reader_reset_io_count();
    double start = get_time_sec();
    long result = disk_nested_loop_join(customer_file, order_file, 1);
    double end = get_time_sec();
    long io_count = disk_reader_get_io_count();
    
    printf("\n  결과:\n");
    printf("    매칭된 레코드: %ld개\n", result);
    printf("    실행 시간: %.3f초\n", end - start);
    printf("    디스크 I/O 횟수: %ld회\n\n", io_count);
}

void run_disk_hash_join_test(const char *customer_file, const char *order_file) {
    printf("[테스트 2: 해시 조인 (Hash Join)]\n");
    
    disk_reader_reset_io_count();
    double start = get_time_sec();
    long result = disk_hash_join(customer_file, order_file);
    double end = get_time_sec();
    long io_count = disk_reader_get_io_count();
    
    printf("  결과:\n");
    printf("    매칭된 레코드: %ld개\n", result);
    printf("    실행 시간: %.3f초\n", end - start);
    printf("    디스크 I/O 횟수: %ld회\n\n", io_count);
}

void run_disk_block_nested_loop_join_test(const char *customer_file, const char *order_file, int buffer_blocks) {
    printf("[테스트 3: 블록 중첩 루프 조인 (Block Nested Loop Join, 버퍼=%d 블록)]\n", buffer_blocks);
    
    disk_reader_reset_io_count();
    double start = get_time_sec();
    long result = disk_block_nested_loop_join(customer_file, order_file, buffer_blocks);
    double end = get_time_sec();
    long io_count = disk_reader_get_io_count();
    
    printf("  결과:\n");
    printf("    매칭된 레코드: %ld개\n", result);
    printf("    실행 시간: %.3f초\n", end - start);
    printf("    디스크 I/O 횟수: %ld회\n\n", io_count);
}

void run_disk_parallel_block_nested_loop_join_test(const char *customer_file, const char *order_file, int buffer_blocks) {
    printf("[테스트 4: 병렬 블록 중첩 루프 조인 (Parallel Block NLJ, 버퍼=%d 블록, 4 스레드)]\n", buffer_blocks);
    
    disk_reader_reset_io_count();
    double start = get_time_sec();
    long result = disk_parallel_block_nested_loop_join(customer_file, order_file, buffer_blocks);
    double end = get_time_sec();
    long io_count = disk_reader_get_io_count();
    
    printf("  결과:\n");
    printf("    매칭된 레코드: %ld개\n", result);
    printf("    실행 시간: %.3f초\n", end - start);
    printf("    디스크 I/O 횟수: %ld회\n\n", io_count);
}

int main() {
    const char *customer_file = "../tbl/customer.tbl";
    const char *order_file = "../tbl/orders.tbl";

    // printf("=== On-Disk JOIN 알고리즘 테스트 ===\n\n");
    // run_disk_hash_join_test(customer_file, order_file);
    // run_disk_block_nested_loop_join_test(customer_file, order_file, 100);
    
    printf("\n=== 병렬 처리 테스트 ===\n\n");
    run_disk_parallel_block_nested_loop_join_test(customer_file, order_file, 100);
    run_disk_parallel_block_nested_loop_join_test(customer_file, order_file, 50);

    return 0;
}
