#ifndef DISK_READER_H
#define DISK_READER_H

#include <stdio.h>

#define BLOCK_SIZE 4096
#define RECORDS_PER_BLOCK 100

typedef struct {
    long custkey;           // C_CUSTKEY
    char name[26];          // C_NAME
    char address[41];       // C_ADDRESS
    long nationkey;         // C_NATIONKEY
    char phone[16];         // C_PHONE
    double acctbal;         // C_ACCTBAL
    char mktsegment[11];    // C_MKTSEGMENT
    char comment[118];      // C_COMMENT
} CustomerRecord;

typedef struct {
    long orderkey;          // O_ORDERKEY
    long custkey;           // O_CUSTKEY
    char orderstatus;       // O_ORDERSTATUS (single char: O, F, P)
    double totalprice;      // O_TOTALPRICE
    char orderdate[11];     // O_ORDERDATE (YYYY-MM-DD)
    char orderpriority[16]; // O_ORDERPRIORITY
    char clerk[16];         // O_CLERK
    long shippriority;      // O_SHIPPRIORITY
    char comment[80];       // O_COMMENT
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
