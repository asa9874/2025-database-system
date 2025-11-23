# Makefile for disk block management system

CC = gcc
CFLAGS = -std=c99 -Wall -Wextra -g
TARGET1 = test_disk_reader
TARGET2 = test_block_reader
TARGET3 = test_block_size
OBJS1 = disk_reader.o test_disk_reader.o
OBJS2 = block_reader.o test_block_reader.o
OBJS3 = block_reader.o test_block_size.o

all: $(TARGET1) $(TARGET2) $(TARGET3)

$(TARGET1): $(OBJS1)
	$(CC) $(CFLAGS) -o $(TARGET1) $(OBJS1)

$(TARGET2): $(OBJS2)
	$(CC) $(CFLAGS) -pthread -o $(TARGET2) $(OBJS2)

$(TARGET3): $(OBJS3)
	$(CC) $(CFLAGS) -o $(TARGET3) $(OBJS3)

disk_reader.o: disk_reader.c disk_reader.h
	$(CC) $(CFLAGS) -c disk_reader.c

test_disk_reader.o: test_disk_reader.c disk_reader.h
	$(CC) $(CFLAGS) -c test_disk_reader.c

block_reader.o: block_reader.c block_reader.h
	$(CC) $(CFLAGS) -c block_reader.c

test_block_reader.o: test_block_reader.c block_reader.h
	$(CC) $(CFLAGS) -c test_block_reader.c

test_block_size.o: test_block_size.c block_reader.h
	$(CC) $(CFLAGS) -c test_block_size.c

clean:
	rm -f $(TARGET1) $(TARGET2) $(TARGET3) $(OBJS1) $(OBJS2) $(OBJS3)

test_disk: $(TARGET1)
	./$(TARGET1)

test_block: $(TARGET2)
	./$(TARGET2)

run_block_size: $(TARGET3)
	./$(TARGET3)

compare: $(TARGET1) $(TARGET2)
	@echo "========================================="
	@echo "기존 disk_reader.c 테스트:"
	@echo "========================================="
	@./$(TARGET1)
	@echo ""
	@echo "========================================="
	@echo "새로운 block_reader.c 테스트:"
	@echo "========================================="
	@./$(TARGET2)

benchmark: $(TARGET3)
	@echo "==========================================="
	@echo "블록 크기 성능 벤치마크"
	@echo "==========================================="
	@./$(TARGET3)

.PHONY: all clean test_disk test_block run_block_size compare benchmark
