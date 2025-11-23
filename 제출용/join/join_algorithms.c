#include <stdio.h>
#include <stdlib.h>
#include <omp.h>  // OpenMP 헤더 추가
#include "join_algorithms.h"
#include "disk_reader.h"
#include "disk_save.h"

#include <stdio.h>
#include <stdlib.h>
#include <omp.h>  // OpenMP 헤더 추가
#include "join_algorithms.h"
#include "disk_reader.h"
#include "disk_save.h"

// ========================================
// OpenMP 기반 병렬 블록 해시 조인 (결과 저장)
// - 독립 스캔 아키텍처: 각 스레드가 독립적으로 파일 I/O 수행
// - Customer 데이터 분할: 각 스레드가 자신의 파트만 처리
// - Order 데이터 중복 스캔: 각 스레드가 전체 Order 파일을 독립적으로 읽음
// - 해시 테이블을 통한 O(1) 탐색으로 빠른 조인 수행
// ========================================

long disk_parallel_block_nested_loop_join_hash_save(const char *customer_file, const char *order_file, 
                                                     int block_size, const char *output_file, int num_threads) {
    // ========================================
    // 1. 출력 파일 초기화 및 준비 단계
    // ========================================
    if (disk_save_init(output_file) != 0) {
        fprintf(stderr, "출력 파일 초기화 실패\n");
        return -1;
    }

    // ========================================
    // 2. 데이터 크기 파악 및 작업 분배 준비
    // ========================================
    // Customer 파일의 총 라인 수 계산 (작업 분배를 위해)
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

    // 각 스레드의 작업 범위 계산 (균등 분배)
    long chunk_size = total_lines / num_threads;

    // ========================================
    // 3. OpenMP 병렬 처리 영역
    // - num_threads: 지정된 스레드 수만큼 병렬 실행
    // - reduction(+:total_result): 각 스레드의 결과를 자동으로 합산
    // ========================================
    long total_result = 0;

    #pragma omp parallel for num_threads(num_threads) reduction(+:total_result)
    for (int i = 0; i < num_threads; i++) {
        // ========================================
        // 3.1 각 스레드의 작업 범위 및 초기화
        // ========================================
        // 각 스레드의 작업 범위 계산
        long start_line = i * chunk_size;
        long end_line = (i == num_threads - 1) ? total_lines : (i + 1) * chunk_size;
        int thread_id = i + 1;
        long result_count = 0;

        printf("[Thread %d] 시작: 라인 %ld ~ %ld (결과 저장 버전)\n",
               thread_id, start_line, end_line);

        // ========================================
        // 3.2 파일 및 메모리 리소스 초기화
        // ========================================
        // 각 스레드별 독립적인 파일 리더 생성 (공유 스캔 아키텍처)
        DiskReader *cust_reader = disk_reader_open(customer_file, "customer", block_size);
        DiskReader *order_reader = disk_reader_open(order_file, "order", block_size);

        if (!cust_reader || !order_reader) {
            fprintf(stderr, "[Thread %d] 파일 열기 실패\n", thread_id);
            if (cust_reader) disk_reader_close(cust_reader);
            if (order_reader) disk_reader_close(order_reader);
            continue;  // 오류 시 다음 스레드로
        }

        // 메모리 버퍼 할당 (블록 단위 I/O를 위한)
        // block_size에 따른 최대 레코드 수 계산
        int max_cust_records = block_size / sizeof(CustomerRecord);
        int max_order_records = block_size / sizeof(OrderRecord);
        CustomerRecord *cust_buffer = (CustomerRecord *)malloc(sizeof(CustomerRecord) * max_cust_records);
        OrderRecord *order_buffer = (OrderRecord *)malloc(sizeof(OrderRecord) * max_order_records);

        if (!cust_buffer || !order_buffer) {
            fprintf(stderr, "[Thread %d] 버퍼 할당 실패\n", thread_id);
            free(cust_buffer);
            free(order_buffer);
            disk_reader_close(cust_reader);
            disk_reader_close(order_reader);
            continue;
        }

        // ========================================
        // 3.3 해시 테이블 및 결과 버퍼 생성
        // ========================================
        // 해시 테이블 생성: Customer 키를 O(1)으로 탐색하기 위한 자료구조
        HashNode **hash_table = (HashNode **)calloc(HASH_SIZE, sizeof(HashNode *));
        if (!hash_table) {
            fprintf(stderr, "[Thread %d] 해시 테이블 할당 실패\n", thread_id);
            free(cust_buffer);
            free(order_buffer);
            disk_reader_close(cust_reader);
            disk_reader_close(order_reader);
            continue;
        }

        // 결과 버퍼 생성: 매칭 결과를 10000개씩 묶어서 디스크에 저장
        ResultBuffer *result_buf = result_buffer_create(output_file, 10000);
        if (!result_buf) {
            fprintf(stderr, "[Thread %d] 결과 버퍼 할당 실패\n", thread_id);
            free(hash_table);
            free(cust_buffer);
            free(order_buffer);
            disk_reader_close(cust_reader);
            disk_reader_close(order_reader);
            continue;
        }

        // ========================================
        // 3.4 작업 범위 시작 위치로 이동
        // ========================================
        // 각 스레드가 담당하는 start_line까지 Customer 파일을 스킵
        CustomerRecord temp;
        for (long j = 0; j < start_line; j++) {
            if (!disk_reader_read_customer(cust_reader, &temp)) {
                break;
            }
        }

        // ========================================
        // 3.5 메인 처리 루프: 블록 단위 해시 조인 수행
        // ========================================
        long current_line = start_line;
        int cust_count;
        long initial_io;

        while (current_line < end_line) {
            // ========================================
            // 3.5.1 Customer 블록 읽기 (외부 루프)
            // ========================================
            cust_count = 0;
            initial_io = disk_reader_get_io_count();

            // Customer 블록 읽기: 메모리 버퍼 크기 또는 I/O 블록 제한까지
            while (cust_count < max_cust_records && current_line < end_line) {
                if (!disk_reader_read_customer(cust_reader, &cust_buffer[cust_count])) {
                    break;
                }
                cust_count++;
                current_line++;

                if (disk_reader_get_io_count() - initial_io >= 1) {
                    break;
                }
            }

            if (cust_count == 0) break;

            // ========================================
            // 3.5.2 해시 테이블 구축 (Customer 데이터를 인덱싱)
            // ========================================
            // 읽은 Customer 블록을 해시 테이블에 삽입하여 빠른 탐색 준비
            for (int j = 0; j < cust_count; j++) {
                long key = cust_buffer[j].custkey;
                int hash = key % HASH_SIZE;

                HashNode *node = (HashNode *)malloc(sizeof(HashNode));
                node->custkey = key;
                node->customer_idx = j;
                node->next = hash_table[hash];
                hash_table[hash] = node;
            }

            // ========================================
            // 3.5.3 Orders 테이블 전체 스캔 및 조인 수행 (내부 루프)
            // ========================================
            disk_reader_reset(order_reader);  // Order 파일을 처음부터 다시 읽기 시작

            // Orders 전체 스캔 (블록 단위로 읽으며 조인 수행)
            int order_count;
            while (1) {
                order_count = 0;
                initial_io = disk_reader_get_io_count();

                // Order 블록 읽기
                while (order_count < max_order_records) {
                    if (!disk_reader_read_order(order_reader, &order_buffer[order_count])) {
                        break;
                    }
                    order_count++;

                    if (disk_reader_get_io_count() - initial_io >= 1) {
                        break;
                    }
                }

                if (order_count == 0) break;

                // ========================================
                // 3.5.4 해시 테이블 탐색 및 결과 매칭/저장
                // ========================================
                // 각 Order 레코드에 대해 해시 테이블에서 Customer 매칭 탐색
                for (int j = 0; j < order_count; j++) {
                    long key = order_buffer[j].custkey;
                    int hash = key % HASH_SIZE;

                    HashNode *node = hash_table[hash];
                    while (node) {
                        if (node->custkey == key) {
                            // 매칭 성공: Customer와 Order 정보를 결과 버퍼에 추가
                            result_buffer_add(result_buf,
                                            &cust_buffer[node->customer_idx],
                                            &order_buffer[j]);
                            result_count++;
                        }
                        node = node->next;
                    }
                }
            }

            // ========================================
            // 3.5.5 해시 테이블 정리 (메모리 누수 방지)
            // ========================================
            // 현재 블록의 해시 테이블을 완전히 해제하여 다음 블록 준비
            for (int j = 0; j < HASH_SIZE; j++) {
                HashNode *node = hash_table[j];
                while (node) {
                    HashNode *temp = node;
                    node = node->next;
                    free(temp);
                }
                hash_table[j] = NULL;
            }
        }

        printf("[Thread %d] 완료: %ld건 매칭 및 저장\n", thread_id, result_count);

        // ========================================
        // 3.6 리소스 정리 및 마무리
        // ========================================
        // 남은 결과 플러시 및 모든 리소스 해제
        result_buffer_destroy(result_buf);
        free(hash_table);
        free(cust_buffer);
        free(order_buffer);
        disk_reader_close(cust_reader);
        disk_reader_close(order_reader);

        total_result += result_count;
    }

    // ========================================
    // 4. 최종 결과 파일 마무리
    // ========================================
    // 출력 파일에 통계 정보 추가 및 최종 정리
    disk_save_finalize(output_file, total_result);

    printf("\n병렬 처리 완료 (결과 저장 버전)!\n");
    return total_result;
}
