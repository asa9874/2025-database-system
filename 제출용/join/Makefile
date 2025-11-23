CC=gcc
CFLAGS=-O3 -Wall -std=c11 -pthread -fopenmp -march=native -ftree-vectorize
LDFLAGS=-pthread -fopenmp

SOURCES=run.c join_algorithms.c disk_reader.c disk_save.c
OBJECTS=$(SOURCES:.c=.o)
HEADERS=join_algorithms.h disk_reader.h disk_save.h

OUT=test_flexible

.PHONY: all clean run

all: $(OUT)

$(OUT): $(OBJECTS)
	$(CC) $(CFLAGS) -o $(OUT) $(OBJECTS) $(LDFLAGS)

%.o: %.c $(HEADERS)
	$(CC) $(CFLAGS) -c $< -o $@

THREADS?=4
run: all
	./$(OUT) $(THREADS)

clean:
	rm -f $(OUT) $(OBJECTS)
