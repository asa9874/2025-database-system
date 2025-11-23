
## 설치 및 빌드

```bash
cd join
make
```

## 사용 방법

### 기본 실행 (8 스레드)
```bash
./run
```

### 스레드 수 지정
```bash
./run [스레드 수]
```

### 입력 파일(tbl 형식 파일을 배치해줘야합니다.)
- Customer: `../tbl/customer.tbl`
- Orders: `../tbl/orders.tbl`

### 출력 파일
- 결과: `./join_results.txt`

## 성능 결과

### 실행 환경
- CPU: 다중 코어 프로세서
- 데이터: 150K 고객 레코드, 1.5M 주문 레코드
