#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include "data_loader.h"
#include "join_algorithms.h"

double get_time_sec() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

void run_nested_loop_join_test() {
    printf("[테스트 1: 중첩 루프 조인 (Nested Loop Join)]\n");
    
    double start = get_time_sec();
    long result = nested_loop_join(1);
    double end = get_time_sec();
    
    printf("\n  결과:\n");
    printf("    매칭된 레코드: %ld개\n", result);
    printf("    실행 시간: %.3f초\n", end - start);
}

void run_hash_join_test() {
    printf("[테스트 2: 해시 조인 (Hash Join)]\n");
    
    double start = get_time_sec();
    long result = hash_join();
    double end = get_time_sec();
    
    printf("  결과:\n");
    printf("    매칭된 레코드: %ld개\n", result);
    printf("    실행 시간: %.3f초\n", end - start);
}

int main() {
    int limit_customers, limit_orders;

    //이건 없앨거(나중에 각 join부분에서 필요한 만큼만 load하게끔)
    if (load_data(limit_customers, limit_orders) != 0) { 
        return 1;
    }
    run_nested_loop_join_test();
    run_hash_join_test();
    cleanup_data();
    return 0;
}
