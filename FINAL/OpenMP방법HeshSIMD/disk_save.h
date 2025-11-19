#ifndef DISK_SAVE_H
#define DISK_SAVE_H

#include "disk_reader.h"

// JOIN 결과를 저장하는 구조체
typedef struct {
    CustomerRecord customer;
    OrderRecord order;
} JoinResult;

// JOIN 결과 버퍼 (각 스레드가 사용)
typedef struct {
    JoinResult *results;
    long capacity;
    long count;
    char output_file[256];
} ResultBuffer;

// 결과 버퍼 초기화
ResultBuffer* result_buffer_create(const char *output_file, long initial_capacity);

// 결과 버퍼에 JOIN 결과 추가
int result_buffer_add(ResultBuffer *buffer, const CustomerRecord *cust, const OrderRecord *ord);

// 버퍼 내용을 디스크에 저장 (append 모드)
int result_buffer_flush(ResultBuffer *buffer);

// 버퍼 해제
void result_buffer_destroy(ResultBuffer *buffer);

// 전역 파일 초기화 (헤더 작성)
int disk_save_init(const char *output_file);

// 전역 파일 finalize (통계 정보 작성)
int disk_save_finalize(const char *output_file, long total_count);

#endif
