
## 설치 및 빌드

```bash
cd join
make
```

## 사용 방법

### 기본 실행 (8 스레드, 16MB 버퍼)
```bash
./run.out
```

### 스레드 수 지정
```bash
./run [스레드 수]
```

### 스레드 수 및 버퍼 크기 지정
```bash
./run [스레드 수] [버퍼 크기 (MB)]
```

### 입력 파일(tbl 형식 파일을 배치해줘야합니다.)
- Customer: `/tbl/customer.tbl`
- Orders: `/tbl/orders.tbl`

### 출력 파일
- 결과: `./join_results.txt`