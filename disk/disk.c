#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "disk.h"

#define BLOCK_SIZE 4096
#define HEADER_SIZE 8
#define MAX_FIELD_LEN 1024
#define MAX_RECORDS 1000

typedef struct {
    char nationkey[32];
    char name[64];
    char regionkey[32];
    char comment[256];
} Record;

typedef struct {
    Record* records;
    size_t count;
    size_t capacity;
} RecordArray;

// RecordArray 초기화
RecordArray* createRecordArray() {
    RecordArray* arr = (RecordArray*)malloc(sizeof(RecordArray));
    arr->capacity = MAX_RECORDS;
    arr->count = 0;
    arr->records = (Record*)malloc(sizeof(Record) * arr->capacity);
    return arr;
}

// RecordArray 해제
void freeRecordArray(RecordArray* arr) {
    if (arr) {
        free(arr->records);
        free(arr);
    }
}

// RecordArray에 레코드 추가
void addRecord(RecordArray* arr, const Record* record) {
    if (arr->count >= arr->capacity) {
        arr->capacity *= 2;
        arr->records = (Record*)realloc(arr->records, sizeof(Record) * arr->capacity);
    }
    arr->records[arr->count++] = *record;
}

// 문자열 분할 함수
void splitString(const char* str, char delimiter, char result[][MAX_FIELD_LEN], int* count) {
    int i = 0, j = 0, field_idx = 0;
    int len = strlen(str);
    
    for (i = 0; i < len; i++) {
        if (str[i] == delimiter) {
            result[field_idx][j] = '\0';
            field_idx++;
            j = 0;
        } else {
            result[field_idx][j++] = str[i];
        }
    }
    result[field_idx][j] = '\0';
    *count = field_idx + 1;
}

// TBL 파일 파싱
RecordArray* parseTblFile(const char* filename) {
    FILE* file = fopen(filename, "r");
    if (!file) {
        printf("파일을 열 수 없습니다: %s\n", filename);
        return NULL;
    }
    
    RecordArray* records = createRecordArray();
    char line[2048];
    
    while (fgets(line, sizeof(line), file)) {
        // 개행 문자 제거
        line[strcspn(line, "\n")] = 0;
        
        if (strlen(line) == 0) continue;
        
        char fields[10][MAX_FIELD_LEN];
        int field_count;
        splitString(line, '|', fields, &field_count);
        
        if (field_count >= 4) {
            Record record;
            strncpy(record.nationkey, fields[0], sizeof(record.nationkey) - 1);
            strncpy(record.name, fields[1], sizeof(record.name) - 1);
            strncpy(record.regionkey, fields[2], sizeof(record.regionkey) - 1);
            strncpy(record.comment, fields[3], sizeof(record.comment) - 1);
            
            record.nationkey[sizeof(record.nationkey) - 1] = '\0';
            record.name[sizeof(record.name) - 1] = '\0';
            record.regionkey[sizeof(record.regionkey) - 1] = '\0';
            record.comment[sizeof(record.comment) - 1] = '\0';
            
            addRecord(records, &record);
        }
    }
    
    fclose(file);
    return records;
}

// 레코드를 바이트로 인코딩
size_t encodeRecord(const Record* record, uint8_t* buffer) {
    size_t offset = 4; // 레코드 길이를 위한 공간 예약
    
    // 4개 필드 인코딩
    const char* fields[] = {
        record->nationkey,
        record->name,
        record->regionkey,
        record->comment
    };
    
    for (int i = 0; i < 4; i++) {
        uint16_t len = strlen(fields[i]);
        buffer[offset++] = len & 0xFF;
        buffer[offset++] = (len >> 8) & 0xFF;
        memcpy(&buffer[offset], fields[i], len);
        offset += len;
    }
    
    // 전체 레코드 길이 저장 (헤더 제외)
    uint32_t record_len = offset - 4;
    buffer[0] = record_len & 0xFF;
    buffer[1] = (record_len >> 8) & 0xFF;
    buffer[2] = (record_len >> 16) & 0xFF;
    buffer[3] = (record_len >> 24) & 0xFF;
    
    return offset;
}

// 바이트에서 레코드 디코딩
size_t decodeRecord(const uint8_t* buffer, Record* record) {
    size_t offset = 0;
    
    // 레코드 길이 읽기
    uint32_t record_len = buffer[offset] | 
                         (buffer[offset + 1] << 8) |
                         (buffer[offset + 2] << 16) |
                         (buffer[offset + 3] << 24);
    offset += 4;
    
    size_t end_offset = offset + record_len;
    char* fields[] = {
        record->nationkey,
        record->name,
        record->regionkey,
        record->comment
    };
    size_t field_sizes[] = {
        sizeof(record->nationkey) - 1,
        sizeof(record->name) - 1,
        sizeof(record->regionkey) - 1,
        sizeof(record->comment) - 1
    };
    
    // 4개 필드 디코딩
    for (int i = 0; i < 4 && offset < end_offset; i++) {
        uint16_t len = buffer[offset] | (buffer[offset + 1] << 8);
        offset += 2;
        
        size_t copy_len = (len < field_sizes[i]) ? len : field_sizes[i];
        memcpy(fields[i], &buffer[offset], copy_len);
        fields[i][copy_len] = '\0';
        offset += len;
    }
    
    return offset;
}

// 레코드들을 블록으로 저장
void writeRecordsToBlocks(const char* filename, const RecordArray* records) {
    FILE* file = fopen(filename, "wb");
    if (!file) {
        printf("파일을 쓸 수 없습니다: %s\n", filename);
        return;
    }
    
    uint8_t* current_block = (uint8_t*)calloc(BLOCK_SIZE, 1);
    uint32_t record_count = 0;
    size_t bytes_used = HEADER_SIZE;
    
    for (size_t i = 0; i < records->count; i++) {
        uint8_t encoded[2048];
        size_t encoded_size = encodeRecord(&records->records[i], encoded);
        
        // 현재 블록에 공간이 충분한지 확인
        if (bytes_used + encoded_size > BLOCK_SIZE) {
            // 블록 헤더 작성
            memcpy(&current_block[0], &record_count, 4);
            memcpy(&current_block[4], &bytes_used, 4);
            
            // 블록 쓰기
            fwrite(current_block, 1, BLOCK_SIZE, file);
            
            // 새 블록 시작
            memset(current_block, 0, BLOCK_SIZE);
            record_count = 0;
            bytes_used = HEADER_SIZE;
        }
        
        // 레코드를 블록에 추가
        memcpy(&current_block[bytes_used], encoded, encoded_size);
        bytes_used += encoded_size;
        record_count++;
    }
    
    // 마지막 블록 쓰기
    if (record_count > 0) {
        memcpy(&current_block[0], &record_count, 4);
        memcpy(&current_block[4], &bytes_used, 4);
        fwrite(current_block, 1, BLOCK_SIZE, file);
    }
    
    free(current_block);
    fclose(file);
    printf("저장 완료: %s\n", filename);
}

// 블록 파일에서 모든 레코드 읽기
RecordArray* readAllRecords(const char* filename) {
    FILE* file = fopen(filename, "rb");
    if (!file) {
        printf("파일을 열 수 없습니다: %s\n", filename);
        return NULL;
    }
    
    RecordArray* records = createRecordArray();
    uint8_t* block = (uint8_t*)malloc(BLOCK_SIZE);
    int block_num = 0;
    
    while (fread(block, 1, BLOCK_SIZE, file) == BLOCK_SIZE) {
        // 블록 헤더 읽기
        uint32_t record_count, bytes_used;
        memcpy(&record_count, &block[0], 4);
        memcpy(&bytes_used, &block[4], 4);
        
        printf("블록 %d: %u개 레코드, %u바이트 사용\n", 
               block_num, record_count, bytes_used);
        
        // 레코드들 읽기
        size_t offset = HEADER_SIZE;
        for (uint32_t i = 0; i < record_count; i++) {
            Record record;
            offset += decodeRecord(&block[offset], &record);
            addRecord(records, &record);
        }
        
        block_num++;
    }
    
    free(block);
    fclose(file);
    return records;
}

// 통계 출력
void printStatistics(const char* filename) {
    FILE* file = fopen(filename, "rb");
    if (!file) return;
    
    fseek(file, 0, SEEK_END);
    long file_size = ftell(file);
    fclose(file);
    
    size_t num_blocks = file_size / BLOCK_SIZE;
    
    printf("\n=== 블록 저장 통계 ===\n");
    printf("파일 크기: %ld 바이트\n", file_size);
    printf("블록 크기: %d 바이트\n", BLOCK_SIZE);
    printf("총 블록 수: %zu\n", num_blocks);
    printf("블록당 평균 사용률: %.1f%%\n", 
           (file_size / (double)(num_blocks * BLOCK_SIZE) * 100));
}

// 레코드 비교
int compareRecords(const Record* r1, const Record* r2) {
    return strcmp(r1->nationkey, r2->nationkey) == 0 &&
           strcmp(r1->name, r2->name) == 0 &&
           strcmp(r1->regionkey, r2->regionkey) == 0 &&
           strcmp(r1->comment, r2->comment) == 0;
}

// main 함수는 test.c로 이동됨