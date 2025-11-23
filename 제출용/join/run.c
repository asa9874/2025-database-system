#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <sys/time.h>
#include "join_algorithms.h"
#include "disk_reader.h"

int main(int argc, char *argv[]) {
    const char *customer_file = "../tbl/customer.tbl";
    const char *order_file = "../tbl/orders.tbl";
    const char *output_file = "./join_results.txt";
    int buffer_blocks = 100;  // I/O 증가를 위한 작은 버퍼
    int num_threads = 8;  // 기본값 
    
    // 명령줄 인자로 스레드 개수 받기 (선택적)
    if (argc > 1) {
        num_threads = atoi(argv[1]);
        if (num_threads <= 0 || num_threads > 32) {
            fprintf(stderr, "유효하지 않은 스레드 개수: %d (1-32 사이로 지정)\n", num_threads);
            return 1;
        }
    }
    
    printf("==============================================\n");
    printf("조인\n");
    printf("==============================================\n\n");
    
    printf("입력 파일:\n");
    printf("  - Customer: %s\n", customer_file);
    printf("  - Orders: %s\n", order_file);
    printf("  - Buffer Blocks: %d\n", buffer_blocks);
    printf("  - 병렬 스레드: %d개\n\n", num_threads);
    
    // I/O 카운터 초기화
    disk_reader_reset_io_count();
    
    // 시작 시간 기록 (실제 시간)
    struct timeval start_time, end_time;
    gettimeofday(&start_time, NULL);
    
    // JOIN 수행 및 결과 저장
    long result_count = disk_parallel_block_nested_loop_join_hash_save(
        customer_file, order_file, buffer_blocks, output_file, num_threads);
    
    // 종료 시간 기록 (실제 시간)
    gettimeofday(&end_time, NULL);
    double elapsed = (end_time.tv_sec - start_time.tv_sec) + 
                     (double)(end_time.tv_usec - start_time.tv_usec) / 1000000.0;
    
    printf("\n==============================================\n");
    printf("실행 결과:\n");
    printf("  - 매칭된 레코드 수: %ld\n", result_count);
    printf("  - 총 I/O 횟수: %ld\n", disk_reader_get_io_count());
    printf("  - 실행 시간: %.2f초\n", elapsed);
    printf("  - 결과 파일: %s\n", output_file);
    printf("==============================================\n");
    
    return 0;
}
