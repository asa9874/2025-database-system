#ifndef DISK_H
#define DISK_H

#include <stdint.h>
#include <stdlib.h>

#define BLOCK_SIZE 4096
#define HEADER_SIZE 8
#define MAX_FIELD_LEN 1024
#define MAX_RECORDS 1000

// 레코드 구조체
typedef struct {
    char nationkey[32];
    char name[64];
    char regionkey[32];
    char comment[256];
} Record;

// 레코드 배열 구조체
typedef struct {
    Record* records;
    size_t count;
    size_t capacity;
} RecordArray;

// RecordArray 관리 함수
RecordArray* createRecordArray();
void freeRecordArray(RecordArray* arr);
void addRecord(RecordArray* arr, const Record* record);

// 파일 처리 함수
RecordArray* parseTblFile(const char* filename);
void writeRecordsToBlocks(const char* filename, const RecordArray* records);
RecordArray* readAllRecords(const char* filename);

// 유틸리티 함수
void printStatistics(const char* filename);
int compareRecords(const Record* r1, const Record* r2);
void splitString(const char* str, char delimiter, char result[][MAX_FIELD_LEN], int* count);

// 인코딩/디코딩 함수
size_t encodeRecord(const Record* record, uint8_t* buffer);
size_t decodeRecord(const uint8_t* buffer, Record* record);

#endif // DISK_H
