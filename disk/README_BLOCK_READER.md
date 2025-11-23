# 블록 단위 디스크 리더 (Block Reader)

## 개요
고정 크기 블록으로 가변 길이 레코드를 효율적으로 읽는 디스크 I/O 시스템입니다.

## 주요 특징

### 1. 블록 단위 읽기
- **고정 크기 블록**: 4096 바이트(BLOCK_SIZE) 단위로 디스크에서 읽기
- **가변 길이 레코드**: CSV 형식의 가변 길이 레코드를 한 줄씩 파싱
- **버퍼링**: 블록을 메모리에 로드하여 여러 레코드를 순차적으로 처리

### 2. 효율적인 I/O
- 한 번의 I/O로 여러 레코드를 읽음 (일반적으로 블록당 20-30개 레코드)
- `fgets()` 기반 라인 단위 읽기보다 I/O 횟수 대폭 감소
- 전역 I/O 카운터로 멀티스레드 환경 지원

### 3. TPC-H 데이터 지원
- Customer 레코드 (custkey, name)
- Order 레코드 (orderkey, custkey, totalprice)
- 파이프(|) 구분자 파싱

## 파일 구조

```
disk/
├── block_reader.h          # 헤더 파일
├── block_reader.c          # 구현 파일
├── test_block_reader.c     # 테스트 프로그램
├── Makefile                # 빌드 설정
└── README_BLOCK_READER.md  # 이 파일
```

## 컴파일 및 실행

```bash
cd disk/
make test_block_reader
./test_block_reader
```

또는:
```bash
make test_block
```

## API 사용법

### 1. 파일 열기
```c
BlockReader *reader = block_reader_open("../tbl/customer.tbl");
if (!reader) {
    fprintf(stderr, "파일 열기 실패\n");
    return -1;
}
```

### 2. 레코드 읽기

#### Customer 레코드
```c
CustomerRecord record;
while (block_reader_read_customer(reader, &record)) {
    printf("custkey: %ld, name: %s\n", record.custkey, record.name);
}
```

#### Order 레코드
```c
OrderRecord record;
while (block_reader_read_order(reader, &record)) {
    printf("orderkey: %ld, custkey: %ld, price: %.2f\n", 
           record.orderkey, record.custkey, record.totalprice);
}
```

### 3. 파일 리셋
```c
block_reader_reset(reader);  // 파일 포인터를 처음으로
```

### 4. I/O 통계 확인
```c
long io_count = block_reader_get_io_count(reader);
printf("I/O 횟수: %ld\n", io_count);
```

### 5. 파일 닫기
```c
block_reader_close(reader);
```

## join_algorithms.c에서 사용하기

### 예제: Nested Loop Join

```c
#include "../disk/block_reader.h"

long disk_nested_loop_join_with_blocks(const char *customer_file, 
                                        const char *order_file) {
    long result_count = 0;
    BlockReader *cust_reader = block_reader_open(customer_file);
    BlockReader *order_reader = block_reader_open(order_file);
    
    if (!cust_reader || !order_reader) {
        fprintf(stderr, "파일 열기 실패\n");
        if (cust_reader) block_reader_close(cust_reader);
        if (order_reader) block_reader_close(order_reader);
        return -1;
    }
    
    CustomerRecord cust;
    OrderRecord ord;
    
    while (block_reader_read_customer(cust_reader, &cust)) {
        block_reader_reset(order_reader);
        
        while (block_reader_read_order(order_reader, &ord)) {
            if (cust.custkey == ord.custkey) {
                result_count++;
            }
        }
    }
    
    printf("총 I/O 횟수: %ld (Customer) + %ld (Orders)\n",
           block_reader_get_io_count(cust_reader),
           block_reader_get_io_count(order_reader));
    
    block_reader_close(cust_reader);
    block_reader_close(order_reader);
    
    return result_count;
}
```

### 예제: Block Nested Loop Join

```c
long disk_block_nested_loop_join_with_blocks(const char *customer_file,
                                               const char *order_file,
                                               int buffer_blocks) {
    long result_count = 0;
    BlockReader *cust_reader = block_reader_open(customer_file);
    BlockReader *order_reader = block_reader_open(order_file);
    
    if (!cust_reader || !order_reader) {
        return -1;
    }
    
    int max_records = buffer_blocks * 200;
    CustomerRecord *cust_buffer = malloc(sizeof(CustomerRecord) * max_records);
    OrderRecord *order_buffer = malloc(sizeof(OrderRecord) * max_records);
    
    long initial_io, cust_count;
    
    while (1) {
        cust_count = 0;
        initial_io = block_reader_get_io_count(cust_reader);
        
        // Customer 블록 읽기
        while (cust_count < max_records) {
            if (!block_reader_read_customer(cust_reader, &cust_buffer[cust_count])) {
                break;
            }
            cust_count++;
            
            // 지정된 블록 수만큼 읽었으면 중단
            if (block_reader_get_io_count(cust_reader) - initial_io >= buffer_blocks) {
                break;
            }
        }
        
        if (cust_count == 0) break;
        
        block_reader_reset(order_reader);
        
        // Orders 스캔
        long order_count;
        while (1) {
            order_count = 0;
            initial_io = block_reader_get_io_count(order_reader);
            
            while (order_count < max_records) {
                if (!block_reader_read_order(order_reader, &order_buffer[order_count])) {
                    break;
                }
                order_count++;
                
                if (block_reader_get_io_count(order_reader) - initial_io >= buffer_blocks) {
                    break;
                }
            }
            
            if (order_count == 0) break;
            
            // 조인 수행
            for (int i = 0; i < cust_count; i++) {
                for (int j = 0; j < order_count; j++) {
                    if (cust_buffer[i].custkey == order_buffer[j].custkey) {
                        result_count++;
                    }
                }
            }
        }
    }
    
    free(cust_buffer);
    free(order_buffer);
    block_reader_close(cust_reader);
    block_reader_close(order_reader);
    
    return result_count;
}
```

## 기존 disk_reader.c와의 차이점

| 특징 | disk_reader.c | block_reader.c |
|------|--------------|----------------|
| 읽기 방식 | `fgets()` 라인 단위 | `fread()` 블록 단위 |
| 버퍼 사용 | 버퍼 정의만 있고 미사용 | 4KB 블록 버퍼 활용 |
| I/O 효율 | 낮음 (레코드당 1회) | 높음 (블록당 20-30개 레코드) |
| 가변 길이 처리 | `fgets()` 자동 처리 | 수동 라인 조립 |
| 멀티스레드 | 전역 카운터만 | 개별 + 전역 카운터 |

## 성능 개선 예상

- **I/O 횟수**: 약 20-30배 감소
- **처리 속도**: 시스템 콜 오버헤드 감소로 더 빠른 처리
- **캐시 효율**: 블록 단위 읽기로 OS 페이지 캐시 활용 향상

## 멀티스레드 지원

```c
// 전역 I/O 카운터 리셋
block_reader_reset_global_io_count();

// 각 스레드에서 독립적으로 사용
BlockReader *reader = block_reader_open(filename);
// ... 작업 수행 ...
block_reader_close(reader);

// 전체 I/O 횟수 확인
long total_io = block_reader_get_global_io_count();
```

## 제한사항

- 레코드가 MAX_LINE_SIZE(512바이트)를 초과하면 잘림
- 순차 읽기만 지원 (랜덤 액세스 불가)
- 현재는 Customer와 Order 레코드만 지원

## 향후 개선 사항

- [ ] 더 큰 레코드 크기 지원
- [ ] 다른 TPC-H 테이블 지원 (lineitem, part 등)
- [ ] 더블 버퍼링으로 I/O와 처리 병렬화
- [ ] Direct I/O 지원 옵션
- [ ] 압축 파일 지원
