CC=gcc
CFLAGS=-O2 -Wall -std=c11

# 소스 파일
SOURCES=test.c data_loader.c join_algorithms.c
OBJECTS=$(SOURCES:.c=.o)
HEADERS=data_loader.h join_algorithms.h

OUT=test

# 데이터(.tbl)가 있는 디렉토리 (기본값: ../tbl)
TBLDIR?=../tbl

# 추가 실행 인자 (예: make run RUNARGS="-b 10 -v")
RUNARGS?=

.PHONY: all run clean

all: $(OUT)

$(OUT): $(OBJECTS)
	$(CC) $(CFLAGS) -o $(OUT) $(OBJECTS)

%.o: %.c $(HEADERS)
	$(CC) $(CFLAGS) -c $< -o $@

# run: 빌드 후 TBLDIR에서 실행. 실행 예: make run RUNARGS="1000 5000"
run: all
	@echo "실행: $(OUT) (작업 디렉토리: $(TBLDIR))"
	cd $(TBLDIR) && $(CURDIR)/$(OUT) $(RUNARGS)

clean:
	rm -f $(OUT) $(OBJECTS)
