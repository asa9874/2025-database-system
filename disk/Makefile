# Makefile for disk block management system

CC = gcc
CFLAGS = -std=c99 -Wall -Wextra -g
TARGET2 = test_block_reader
OBJS2 = block_reader.o test_block_reader.o

all: $(TARGET2)

$(TARGET2): $(OBJS2)
	$(CC) $(CFLAGS) -pthread -o $(TARGET2) $(OBJS2)

block_reader.o: block_reader.c block_reader.h
	$(CC) $(CFLAGS) -c block_reader.c

test_block_reader.o: test_block_reader.c block_reader.h
	$(CC) $(CFLAGS) -c test_block_reader.c

clean:
	rm -f $(TARGET2) $(OBJS2)

test_block: $(TARGET2)
	./$(TARGET2)

.PHONY: all clean test_block
