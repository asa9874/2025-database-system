#ifndef DISK_READER_H
#define DISK_READER_H

#include <stdio.h>

#define BLOCK_SIZE 4096
#define RECORDS_PER_BLOCK 100

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
    int buffer_valid;
    long current_block;
    long total_blocks;
    int records_in_buffer;
    int current_record;
} DiskReader;

DiskReader* disk_reader_open(const char *filename, const char *type);
int disk_reader_read_customer(DiskReader *reader, CustomerRecord *record);
int disk_reader_read_order(DiskReader *reader, OrderRecord *record);
void disk_reader_reset(DiskReader *reader);
void disk_reader_close(DiskReader *reader);
long disk_reader_get_io_count(void);
void disk_reader_reset_io_count(void);

#endif
