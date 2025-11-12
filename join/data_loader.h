#ifndef DATA_LOADER_H
#define DATA_LOADER_H

#define MAX_CUSTOMERS 150000
#define MAX_ORDERS 15000
#define MAX_LINE_LEN 512

// 고객 구조체
typedef struct {
    long custkey;
    char name[26];
} Customer;

// 주문 구조체
typedef struct {
    long orderkey;
    long custkey;
    double totalprice;
} Order;

// 전역 데이터
extern Customer *customers;
extern Order *orders;
extern int num_customers;
extern int num_orders;

// 함수 선언
int load_customers(const char *filename, int limit);
int load_orders(const char *filename, int limit);
int load_data(int limit_customers, int limit_orders);
void cleanup_data(void);

#endif // DATA_LOADER_H
