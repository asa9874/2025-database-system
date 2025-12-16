실행 환경본 프로젝트는 아래 사양의 시스템에서 테스트 및 실행되었습니다.

* **OS:** Ubuntu 24.04.1 LTS (WSL2)
* **Kernel:** 6.6.87.2-microsoft-standard-WSL2
* **CPU:** Intel Core Ultra 7 258V (8 logical CPUs)
* **Memory:** Total ~15 GiB (Avail ~13 GiB)
* **Compiler:** GCC 13.3.0

## 데이터 생성 및 준비
TPC-H `dbgen` 도구를 사용하여 테스트 데이터를 생성합니다. (Scale Factor: 6)

```bash
# Customer 데이터 생성
./dbgen -s 6 -T c -f 

# Orders 데이터 생성
./dbgen -s 6 -T o -f

```

### 입력 파일 배치생성된 `tbl` 파일은 아래 경로에 위치해야 합니다.

* **Customer:** `/tbl/customer.tbl`
* **Orders:** `/tbl/orders.tbl`

## 설치 및 빌드
```bash
cd join
make
```

## 사용 방법
### 기본 실행기본 설정(8 스레드, 16MB 버퍼)으로 실행합니다.
```bash
./run.out

```

### 스레드 수 지정
```bash
./run [스레드 수]

```

###
스레드 수 및 버퍼 크기 지정
```bash
./run [스레드 수] [버퍼 크기 (MB)]

```

### 출력 파일실행 결과는 아래 파일에 저장됩니다.

* **결과:** `./join_results.txt`
