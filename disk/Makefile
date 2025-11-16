# Makefile for disk block management system

CC = gcc
CFLAGS = -std=c99 -Wall -Wextra -g
TARGET = test
OBJS = test.o disk.o

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS)

test.o: test.c disk.h
	$(CC) $(CFLAGS) -c test.c

disk.o: disk.c disk.h
	$(CC) $(CFLAGS) -c disk.c

clean:
	rm -f $(TARGET) $(OBJS)

run: $(TARGET)
	./$(TARGET)

.PHONY: all clean run
