#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "disk_reader.h"

static long total_io_count = 0;

DiskReader* disk_reader_open(const char *filename, const char *type) {
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
    
    reader->buffer_valid = 0;
    reader->current_block = 0;
    reader->records_in_buffer = 0;
    reader->current_record = 0;
    
    return reader;
}

static int load_block(DiskReader *reader) {
    size_t bytes_read = fread(reader->buffer, 1, BLOCK_SIZE, reader->file);
    if (bytes_read == 0) {
        return 0;
    }
    
    total_io_count++;
    reader->buffer_valid = 1;
    reader->current_block++;
    reader->current_record = 0;
    reader->records_in_buffer = 0;
    
    for (size_t i = 0; i < bytes_read; i++) {
        if (reader->buffer[i] == '\n') {
            reader->records_in_buffer++;
        }
    }
    
    return 1;
}

int disk_reader_read_customer(DiskReader *reader, CustomerRecord *record) {
    char line[512];
    
    if (fgets(line, sizeof(line), reader->file) == NULL) {
        return 0;
    }
    
    long current_pos = ftell(reader->file);
    long current_block = current_pos / BLOCK_SIZE;
    if (reader->current_block != current_block) {
        __sync_fetch_and_add(&total_io_count, 1);
        reader->current_block = current_block;
    }
    
    char *saveptr;
    char *token = strtok_r(line, "|", &saveptr);
    if (token) record->custkey = atol(token);
    
    token = strtok_r(NULL, "|", &saveptr);
    if (token) {
        strncpy(record->name, token, 25);
        record->name[25] = '\0';
    }
    
    return 1;
}

int disk_reader_read_order(DiskReader *reader, OrderRecord *record) {
    char line[512];
    
    if (fgets(line, sizeof(line), reader->file) == NULL) {
        return 0;
    }
    
    long current_pos = ftell(reader->file);
    long current_block = current_pos / BLOCK_SIZE;
    if (reader->current_block != current_block) {
        __sync_fetch_and_add(&total_io_count, 1);
        reader->current_block = current_block;
    }
    
    char *saveptr;
    char *token = strtok_r(line, "|", &saveptr);
    if (token) record->orderkey = atol(token);
    
    token = strtok_r(NULL, "|", &saveptr);
    if (token) record->custkey = atol(token);
    
    token = strtok_r(NULL, "|", &saveptr);
    token = strtok_r(NULL, "|", &saveptr);
    if (token) {
        char *endptr;
        record->totalprice = strtod(token, &endptr);
    }
    
    return 1;
}

void disk_reader_reset(DiskReader *reader) {
    fseek(reader->file, 0, SEEK_SET);
    reader->buffer_valid = 0;
    reader->current_block = 0;
    reader->records_in_buffer = 0;
    reader->current_record = 0;
}

void disk_reader_close(DiskReader *reader) {
    if (reader) {
        if (reader->file) {
            fclose(reader->file);
        }
        free(reader);
    }
}

long disk_reader_get_io_count(void) {
    return total_io_count;
}

void disk_reader_reset_io_count(void) {
    total_io_count = 0;
}
