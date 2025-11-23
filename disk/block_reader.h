#ifndef BLOCK_READER_H
#define BLOCK_READER_H

#include <stdio.h>

#define BLOCK_SIZE 4096
#define MAX_LINE_SIZE 512

typedef struct {
    long custkey;
    char name[26];
} CustomerRecord;

typedef struct {
    long orderkey;
    long custkey;
    double totalprice;
} OrderRecord;

typedef struct {
    FILE *file;
    char buffer[BLOCK_SIZE];
    int buffer_size;        // 버퍼에 실제로 읽은 바이트 수
    int buffer_pos;         // 버퍼 내 현재 읽기 위치
    long current_block;     // 현재 블록 번호
    long total_io_count;    // 총 I/O 횟수
    char line_buffer[MAX_LINE_SIZE];  // 레코드 조립용 버퍼
    int line_pos;           // line_buffer 내 위치
} BlockReader;

// BlockReader 생성 및 해제
BlockReader* block_reader_open(const char *filename);
void block_reader_close(BlockReader *reader);

// 레코드 읽기 함수
int block_reader_read_customer(BlockReader *reader, CustomerRecord *record);
int block_reader_read_order(BlockReader *reader, OrderRecord *record);

// 파일 포인터 리셋
void block_reader_reset(BlockReader *reader);

// I/O 통계
long block_reader_get_io_count(BlockReader *reader);
void block_reader_reset_io_count(BlockReader *reader);

// 전역 I/O 카운터 (멀티스레드용)
long block_reader_get_global_io_count(void);
void block_reader_reset_global_io_count(void);

#endif
