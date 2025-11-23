#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "disk_reader.h"

// ========================================
// 디스크 리더 모듈 (Disk Reader Module)
// - 블록 단위 파일 읽기로 I/O 효율성 향상
// - Customer/Order 레코드 파싱 및 구조체 변환
// - I/O 카운트 추적으로 성능 모니터링
// ========================================

static long total_io_count = 0;

// ========================================
// 1. 디스크 리더 생성 및 초기화
// ========================================

DiskReader* disk_reader_open(const char *filename, const char *type) {
    // 파일 열기 및 DiskReader 구조체 초기화
    DiskReader *reader = (DiskReader *)malloc(sizeof(DiskReader));
    if (!reader) {
        fprintf(stderr, "메모리 할당 실패\n");
        return NULL;
    }

    reader->file = fopen(filename, "r");
    if (!reader->file) {
        perror("파일 열기 실패");
        free(reader);
        return NULL;
    }

    // 초기 상태 설정
    reader->buffer_valid = 0;
    reader->current_block = 0;
    reader->records_in_buffer = 0;
    reader->current_record = 0;

    return reader;
}

// ========================================
// 2. 블록 단위 데이터 로딩 (내부 함수)
// ========================================

static int load_block(DiskReader *reader) {
    // 블록 단위로 파일에서 데이터 읽기
    size_t bytes_read = fread(reader->buffer, 1, BLOCK_SIZE, reader->file);
    if (bytes_read == 0) {
        return 0;  // 읽을 데이터 없음
    }

    // I/O 카운트 증가 및 상태 업데이트
    total_io_count++;
    reader->buffer_valid = 1;
    reader->current_block++;
    reader->current_record = 0;
    reader->records_in_buffer = 0;

    // 버퍼 내 레코드 수 계산 (줄바꿈 기준)
    for (size_t i = 0; i < bytes_read; i++) {
        if (reader->buffer[i] == '\n') {
            reader->records_in_buffer++;
        }
    }

    return 1;
}

// ========================================
// 3. Customer 레코드 읽기 및 파싱
// ========================================

int disk_reader_read_customer(DiskReader *reader, CustomerRecord *record) {
    char line[512];

    // 파일에서 한 줄 읽기
    if (fgets(line, sizeof(line), reader->file) == NULL) {
        return 0;  // EOF 또는 오류
    }

    // I/O 블록 경계 체크 및 카운트
    long current_pos = ftell(reader->file);
    long current_block = current_pos / BLOCK_SIZE;
    if (reader->current_block != current_block) {
        __sync_fetch_and_add(&total_io_count, 1);
        reader->current_block = current_block;
    }

    // ========================================
    // Customer 레코드 파싱 (TPC-H 스키마)
    // Format: CUSTKEY|NAME|ADDRESS|NATIONKEY|PHONE|ACCTBAL|MKTSEGMENT|COMMENT
    // ========================================
    char *saveptr;
    char *token;

    // C_CUSTKEY (고객 키)
    token = strtok_r(line, "|", &saveptr);
    if (token) record->custkey = atol(token);

    // C_NAME (고객 이름)
    token = strtok_r(NULL, "|", &saveptr);
    if (token) {
        strncpy(record->name, token, 25);
        record->name[25] = '\0';
    }

    // C_ADDRESS (주소)
    token = strtok_r(NULL, "|", &saveptr);
    if (token) {
        strncpy(record->address, token, 40);
        record->address[40] = '\0';
    }

    // C_NATIONKEY (국가 키)
    token = strtok_r(NULL, "|", &saveptr);
    if (token) record->nationkey = atol(token);

    // C_PHONE (전화번호)
    token = strtok_r(NULL, "|", &saveptr);
    if (token) {
        strncpy(record->phone, token, 15);
        record->phone[15] = '\0';
    }

    // C_ACCTBAL (계좌 잔액)
    token = strtok_r(NULL, "|", &saveptr);
    if (token) record->acctbal = strtod(token, NULL);

    // C_MKTSEGMENT (시장 세그먼트)
    token = strtok_r(NULL, "|", &saveptr);
    if (token) {
        strncpy(record->mktsegment, token, 10);
        record->mktsegment[10] = '\0';
    }

    // C_COMMENT (코멘트)
    token = strtok_r(NULL, "|", &saveptr);
    if (token) {
        strncpy(record->comment, token, 117);
        record->comment[117] = '\0';
    }

    return 1;
}

// ========================================
// 4. Order 레코드 읽기 및 파싱
// ========================================

int disk_reader_read_order(DiskReader *reader, OrderRecord *record) {
    char line[512];

    // 파일에서 한 줄 읽기
    if (fgets(line, sizeof(line), reader->file) == NULL) {
        return 0;  // EOF 또는 오류
    }

    // I/O 블록 경계 체크 및 카운트
    long current_pos = ftell(reader->file);
    long current_block = current_pos / BLOCK_SIZE;
    if (reader->current_block != current_block) {
        __sync_fetch_and_add(&total_io_count, 1);
        reader->current_block = current_block;
    }

    // ========================================
    // Order 레코드 파싱 (TPC-H 스키마)
    // Format: ORDERKEY|CUSTKEY|ORDERSTATUS|TOTALPRICE|ORDERDATE|ORDERPRIORITY|CLERK|SHIPPRIORITY|COMMENT
    // ========================================
    char *saveptr;
    char *token;

    // O_ORDERKEY (주문 키)
    token = strtok_r(line, "|", &saveptr);
    if (token) record->orderkey = atol(token);

    // O_CUSTKEY (고객 키 - 조인 키)
    token = strtok_r(NULL, "|", &saveptr);
    if (token) record->custkey = atol(token);

    // O_ORDERSTATUS (주문 상태)
    token = strtok_r(NULL, "|", &saveptr);
    if (token) record->orderstatus = token[0];

    // O_TOTALPRICE (총 가격)
    token = strtok_r(NULL, "|", &saveptr);
    if (token) record->totalprice = strtod(token, NULL);

    // O_ORDERDATE (주문 날짜)
    token = strtok_r(NULL, "|", &saveptr);
    if (token) {
        strncpy(record->orderdate, token, 10);
        record->orderdate[10] = '\0';
    }

    // O_ORDERPRIORITY (주문 우선순위)
    token = strtok_r(NULL, "|", &saveptr);
    if (token) {
        strncpy(record->orderpriority, token, 15);
        record->orderpriority[15] = '\0';
    }

    // O_CLERK (담당 직원)
    token = strtok_r(NULL, "|", &saveptr);
    if (token) {
        strncpy(record->clerk, token, 15);
        record->clerk[15] = '\0';
    }

    // O_SHIPPRIORITY (배송 우선순위)
    token = strtok_r(NULL, "|", &saveptr);
    if (token) record->shippriority = atol(token);

    // O_COMMENT (코멘트)
    token = strtok_r(NULL, "|", &saveptr);
    if (token) {
        strncpy(record->comment, token, 79);
        record->comment[79] = '\0';
    }

    return 1;
}

// ========================================
// 5. 파일 포인터 리셋 (재스캔용)
// ========================================

void disk_reader_reset(DiskReader *reader) {
    // 파일 포인터를 처음으로 되돌림 (Order 재스캔용)
    fseek(reader->file, 0, SEEK_SET);

    // 상태 초기화
    reader->buffer_valid = 0;
    reader->current_block = 0;
    reader->records_in_buffer = 0;
    reader->current_record = 0;
}

// ========================================
// 6. 리소스 정리
// ========================================

void disk_reader_close(DiskReader *reader) {
    if (reader) {
        if (reader->file) {
            fclose(reader->file);  // 파일 닫기
        }
        free(reader);  // 구조체 메모리 해제
    }
}

// ========================================
// 7. I/O 카운트 조회 및 관리
// ========================================

long disk_reader_get_io_count(void) {
    return total_io_count;
}

void disk_reader_reset_io_count(void) {
    total_io_count = 0;
}
