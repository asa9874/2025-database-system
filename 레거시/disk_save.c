#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "disk_save.h"

// 파일 쓰기를 위한 전역 뮤텍스 (여러 스레드가 동시에 쓰지 않도록)
static pthread_mutex_t file_mutex = PTHREAD_MUTEX_INITIALIZER;

ResultBuffer* result_buffer_create(const char *output_file, long initial_capacity) {
    ResultBuffer *buffer = (ResultBuffer *)malloc(sizeof(ResultBuffer));
    if (!buffer) {
        fprintf(stderr, "ResultBuffer 할당 실패\n");
        return NULL;
    }
    
    buffer->results = (JoinResult *)malloc(sizeof(JoinResult) * initial_capacity);
    if (!buffer->results) {
        fprintf(stderr, "ResultBuffer 배열 할당 실패\n");
        free(buffer);
        return NULL;
    }
    
    buffer->capacity = initial_capacity;
    buffer->count = 0;
    strncpy(buffer->output_file, output_file, sizeof(buffer->output_file) - 1);
    buffer->output_file[sizeof(buffer->output_file) - 1] = '\0';
    
    return buffer;
}

int result_buffer_add(ResultBuffer *buffer, const CustomerRecord *cust, const OrderRecord *ord) {
    if (!buffer || !cust || !ord) {
        return -1;
    }
    
    // 버퍼가 가득 찼으면 디스크에 플러시
    if (buffer->count >= buffer->capacity) {
        if (result_buffer_flush(buffer) != 0) {
            return -1;
        }
    }
    
    // 결과 추가
    buffer->results[buffer->count].customer = *cust;
    buffer->results[buffer->count].order = *ord;
    buffer->count++;
    
    return 0;
}

int result_buffer_flush(ResultBuffer *buffer) {
    if (!buffer || buffer->count == 0) {
        return 0;  // 쓸 내용이 없으면 성공으로 처리
    }
    
    // 파일 쓰기 시 뮤텍스 잠금 (다중 스레드 환경에서 안전하게)
    pthread_mutex_lock(&file_mutex);
    
    FILE *fp = fopen(buffer->output_file, "a");
    if (!fp) {
        perror("fopen (flush)");
        pthread_mutex_unlock(&file_mutex);
        return -1;
    }
    
    // 각 JOIN 결과를 파일에 기록 (모든 컬럼 포함)
    for (long i = 0; i < buffer->count; i++) {
        JoinResult *result = &buffer->results[i];
        
        // Customer 컬럼 | Order 컬럼
        fprintf(fp, "%ld|%s|%s|%ld|%s|%.2f|%s|%s|"
                    "%ld|%c|%.2f|%s|%s|%s|%ld|%s\n",
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
    pthread_mutex_unlock(&file_mutex);
    
    // 버퍼 초기화
    buffer->count = 0;
    
    return 0;
}

void result_buffer_destroy(ResultBuffer *buffer) {
    if (!buffer) {
        return;
    }
    
    // 남은 데이터가 있으면 플러시
    if (buffer->count > 0) {
        result_buffer_flush(buffer);
    }
    
    free(buffer->results);
    free(buffer);
}

int disk_save_init(const char *output_file) {
    FILE *fp = fopen(output_file, "w");
    if (!fp) {
        perror("fopen (init)");
        return -1;
    }
    
    // 헤더 작성
    fprintf(fp, "# JOIN Results: Customer JOIN Orders\n");
    fprintf(fp, "# Format: C_CUSTKEY|C_NAME|C_ADDRESS|C_NATIONKEY|C_PHONE|C_ACCTBAL|C_MKTSEGMENT|C_COMMENT|"
                "O_ORDERKEY|O_ORDERSTATUS|O_TOTALPRICE|O_ORDERDATE|O_ORDERPRIORITY|O_CLERK|O_SHIPPRIORITY|O_COMMENT\n");
    fprintf(fp, "# ================================================\n");
    
    fclose(fp);
    return 0;
}

int disk_save_finalize(const char *output_file, long total_count) {
    FILE *fp = fopen(output_file, "a");
    if (!fp) {
        perror("fopen (finalize)");
        return -1;
    }
    
    // 마지막에 통계 정보 추가
    fprintf(fp, "# ================================================\n");
    fprintf(fp, "# Total JOIN results: %ld rows\n", total_count);
    
    fclose(fp);
    
    printf("\nJOIN 결과가 '%s' 파일에 저장되었습니다.\n", output_file);
    printf("총 %ld개의 매칭 결과가 저장되었습니다.\n", total_count);
    
    return 0;
}
