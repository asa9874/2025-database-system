#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "disk_save.h"

// ========================================
// 디스크 저장 모듈 (Disk Save Module)
// - 버퍼링을 통한 효율적인 파일 쓰기
// - 다중 스레드 환경에서의 안전한 파일 접근
// - JOIN 결과의 구조화된 저장
// ========================================

// 파일 쓰기를 위한 전역 뮤텍스 (여러 스레드가 동시에 쓰지 않도록)
static pthread_mutex_t file_mutex = PTHREAD_MUTEX_INITIALIZER;

// ========================================
// 1. 결과 버퍼 생성 및 초기화
// ========================================

ResultBuffer* result_buffer_create(const char *output_file, long initial_capacity) {
    // ResultBuffer 구조체 메모리 할당
    ResultBuffer *buffer = (ResultBuffer *)malloc(sizeof(ResultBuffer));
    if (!buffer) {
        fprintf(stderr, "ResultBuffer 할당 실패\n");
        return NULL;
    }

    // 결과 배열 메모리 할당
    buffer->results = (JoinResult *)malloc(sizeof(JoinResult) * initial_capacity);
    if (!buffer->results) {
        fprintf(stderr, "ResultBuffer 배열 할당 실패\n");
        free(buffer);
        return NULL;
    }

    // 초기 설정
    buffer->capacity = initial_capacity;
    buffer->count = 0;
    strncpy(buffer->output_file, output_file, sizeof(buffer->output_file) - 1);
    buffer->output_file[sizeof(buffer->output_file) - 1] = '\0';

    return buffer;
}

// ========================================
// 2. 결과 추가 및 버퍼 관리
// ========================================

int result_buffer_add(ResultBuffer *buffer, const CustomerRecord *cust, const OrderRecord *ord) {
    if (!buffer || !cust || !ord) {
        return -1;  // 유효성 검사
    }

    // ========================================
    // 버퍼가 가득 찼으면 디스크에 플러시
    // ========================================
    if (buffer->count >= buffer->capacity) {
        if (result_buffer_flush(buffer) != 0) {
            return -1;
        }
    }

    // 결과 추가 (메모리에 저장)
    buffer->results[buffer->count].customer = *cust;
    buffer->results[buffer->count].order = *ord;
    buffer->count++;

    return 0;
}

// ========================================
// 3. 버퍼 내용을 디스크에 플러시
// ========================================

int result_buffer_flush(ResultBuffer *buffer) {
    if (!buffer || buffer->count == 0) {
        return 0;  // 쓸 내용이 없으면 성공
    }

    // ========================================
    // 다중 스레드 환경에서 안전한 파일 쓰기
    // ========================================
    pthread_mutex_lock(&file_mutex);  // 뮤텍스 잠금

    FILE *fp = fopen(buffer->output_file, "a");  // append 모드로 열기
    if (!fp) {
        perror("fopen (flush)");
        pthread_mutex_unlock(&file_mutex);
        return -1;
    }

    // ========================================
    // 각 JOIN 결과를 파일에 기록 (TPC-H 포맷)
    // Format: Customer 컬럼들 | Order 컬럼들
    // ========================================
    for (long i = 0; i < buffer->count; i++) {
        JoinResult *result = &buffer->results[i];

        fprintf(fp, "%ld|%s|%s|%ld|%s|%.2f|%s|%s|"           // Customer 필드
                    "%ld|%c|%.2f|%s|%s|%s|%ld|%s\n",        // Order 필드
                // Customer fields
                result->customer.custkey,
                result->customer.name,
                result->customer.address,
                result->customer.nationkey,
                result->customer.phone,
                result->customer.acctbal,
                result->customer.mktsegment,
                result->customer.comment,
                // Order fields
                result->order.orderkey,
                result->order.orderstatus,
                result->order.totalprice,
                result->order.orderdate,
                result->order.orderpriority,
                result->order.clerk,
                result->order.shippriority,
                result->order.comment);
    }

    fclose(fp);
    pthread_mutex_unlock(&file_mutex);  // 뮤텍스 해제

    // 버퍼 초기화 (다음 사용 준비)
    buffer->count = 0;

    return 0;
}

// ========================================
// 4. 결과 버퍼 정리 및 최종 플러시
// ========================================

void result_buffer_destroy(ResultBuffer *buffer) {
    if (!buffer) {
        return;
    }

    // ========================================
    // 남은 데이터가 있으면 최종 플러시
    // ========================================
    if (buffer->count > 0) {
        result_buffer_flush(buffer);
    }

    // 메모리 해제
    free(buffer->results);
    free(buffer);
}

// ========================================
// 5. 출력 파일 초기화
// ========================================

int disk_save_init(const char *output_file) {
    FILE *fp = fopen(output_file, "w");  // 쓰기 모드로 새 파일 생성
    if (!fp) {
        perror("fopen (init)");
        return -1;
    }

    // ========================================
    // 파일 헤더 작성 (메타데이터)
    // ========================================
    fprintf(fp, "# JOIN Results: Customer JOIN Orders\n");
    fprintf(fp, "# Format: C_CUSTKEY|C_NAME|C_ADDRESS|C_NATIONKEY|C_PHONE|C_ACCTBAL|C_MKTSEGMENT|C_COMMENT|");
    fprintf(fp, "O_ORDERKEY|O_ORDERSTATUS|O_TOTALPRICE|O_ORDERDATE|O_ORDERPRIORITY|O_CLERK|O_SHIPPRIORITY|O_COMMENT\n");
    fprintf(fp, "# ================================================\n");

    fclose(fp);
    return 0;
}

// ========================================
// 6. 출력 파일 마무리 및 통계 추가
// ========================================

int disk_save_finalize(const char *output_file, long total_count) {
    FILE *fp = fopen(output_file, "a");  // append 모드로 열기
    if (!fp) {
        perror("fopen (finalize)");
        return -1;
    }

    // ========================================
    // 파일 끝에 통계 정보 추가
    // ========================================
    fprintf(fp, "# ================================================\n");
    fprintf(fp, "# Total JOIN results: %ld rows\n", total_count);

    fclose(fp);

    // 사용자에게 결과 알림
    printf("\nJOIN 결과가 '%s' 파일에 저장되었습니다.\n", output_file);
    printf("총 %ld개의 매칭 결과가 저장되었습니다.\n", total_count);

    return 0;
}
