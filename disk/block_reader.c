#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "block_reader.h"

// 전역 I/O 카운터 (멀티스레드 환경용)
static long global_io_count = 0;

BlockReader* block_reader_open(const char *filename) {
    BlockReader *reader = (BlockReader *)malloc(sizeof(BlockReader));
    if (!reader) {
        fprintf(stderr, "BlockReader 메모리 할당 실패\n");
        return NULL;
    }
    
    reader->file = fopen(filename, "rb");  // 바이너리 모드로 열기
    if (!reader->file) {
        perror("파일 열기 실패");
        free(reader);
        return NULL;
    }
    
    reader->buffer_size = 0;
    reader->buffer_pos = 0;
    reader->current_block = 0;
    reader->total_io_count = 0;
    reader->line_pos = 0;
    memset(reader->buffer, 0, BLOCK_SIZE);
    memset(reader->line_buffer, 0, MAX_LINE_SIZE);
    
    return reader;
}

void block_reader_close(BlockReader *reader) {
    if (reader) {
        if (reader->file) {
            fclose(reader->file);
        }
        free(reader);
    }
}

// 블록 단위로 데이터를 버퍼에 로드
static int load_block(BlockReader *reader) {
    reader->buffer_size = fread(reader->buffer, 1, BLOCK_SIZE, reader->file);
    if (reader->buffer_size == 0) {
        return 0;  // 파일 끝
    }
    
    reader->buffer_pos = 0;
    reader->current_block++;
    reader->total_io_count++;
    __sync_fetch_and_add(&global_io_count, 1);  // 전역 카운터 증가
    
    return 1;
}

// 버퍼에서 한 줄(레코드)을 읽기 (가변 길이 레코드 처리)
static int read_line(BlockReader *reader, char *line, int max_size) {
    int line_idx = 0;
    
    while (line_idx < max_size - 1) {
        // 버퍼가 비었으면 새 블록 로드
        if (reader->buffer_pos >= reader->buffer_size) {
            if (!load_block(reader)) {
                // 파일 끝: 버퍼에 남은 데이터 처리
                if (line_idx > 0) {
                    line[line_idx] = '\0';
                    return 1;
                }
                return 0;
            }
        }
        
        char ch = reader->buffer[reader->buffer_pos++];
        
        if (ch == '\n') {
            line[line_idx] = '\0';
            return 1;
        }
        
        line[line_idx++] = ch;
    }
    
    line[max_size - 1] = '\0';
    return 1;
}

// Customer 레코드 파싱 및 읽기
int block_reader_read_customer(BlockReader *reader, CustomerRecord *record) {
    char line[MAX_LINE_SIZE];
    
    if (!read_line(reader, line, MAX_LINE_SIZE)) {
        return 0;
    }
    
    // 빈 줄 처리
    if (strlen(line) == 0) {
        return block_reader_read_customer(reader, record);
    }
    
    // "|" 구분자로 파싱
    char *saveptr;
    char *token = strtok_r(line, "|", &saveptr);
    if (token) {
        record->custkey = atol(token);
    } else {
        return 0;
    }
    
    token = strtok_r(NULL, "|", &saveptr);
    if (token) {
        strncpy(record->name, token, 25);
        record->name[25] = '\0';
    } else {
        record->name[0] = '\0';
    }
    
    return 1;
}

// Order 레코드 파싱 및 읽기
int block_reader_read_order(BlockReader *reader, OrderRecord *record) {
    char line[MAX_LINE_SIZE];
    
    if (!read_line(reader, line, MAX_LINE_SIZE)) {
        return 0;
    }
    
    // 빈 줄 처리
    if (strlen(line) == 0) {
        return block_reader_read_order(reader, record);
    }
    
    // "|" 구분자로 파싱
    char *saveptr;
    char *token = strtok_r(line, "|", &saveptr);
    if (token) {
        record->orderkey = atol(token);
    } else {
        return 0;
    }
    
    token = strtok_r(NULL, "|", &saveptr);
    if (token) {
        record->custkey = atol(token);
    } else {
        return 0;
    }
    
    // 세 번째 필드 스킵 (orderstatus)
    token = strtok_r(NULL, "|", &saveptr);
    
    // 네 번째 필드: totalprice
    token = strtok_r(NULL, "|", &saveptr);
    if (token) {
        char *endptr;
        record->totalprice = strtod(token, &endptr);
    } else {
        record->totalprice = 0.0;
    }
    
    return 1;
}

// 파일 포인터를 처음으로 리셋
void block_reader_reset(BlockReader *reader) {
    fseek(reader->file, 0, SEEK_SET);
    reader->buffer_size = 0;
    reader->buffer_pos = 0;
    reader->current_block = 0;
    reader->line_pos = 0;
    memset(reader->buffer, 0, BLOCK_SIZE);
    memset(reader->line_buffer, 0, MAX_LINE_SIZE);
}

// I/O 통계 함수들
long block_reader_get_io_count(BlockReader *reader) {
    return reader->total_io_count;
}

void block_reader_reset_io_count(BlockReader *reader) {
    reader->total_io_count = 0;
}

long block_reader_get_global_io_count(void) {
    return global_io_count;
}

void block_reader_reset_global_io_count(void) {
    global_io_count = 0;
}
