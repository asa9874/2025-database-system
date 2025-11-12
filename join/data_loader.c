#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "data_loader.h"

Customer *customers = NULL;
Order *orders = NULL;
int num_customers = 0;
int num_orders = 0;
//이 파일은 없앨거임 JOIN 테스트용 파일
int load_customers(const char *filename, int limit) {
    FILE *fp = fopen(filename, "r");
    if (!fp) {
        perror("customer.tbl 파일 열기 실패");
        return -1;
    }

    int max_load = (limit > 0 && limit < MAX_CUSTOMERS) ? limit : MAX_CUSTOMERS;
    customers = (Customer *)malloc(max_load * sizeof(Customer));
    if (!customers) {
        fprintf(stderr, "메모리 할당 실패\n");
        fclose(fp);
        return -1;
    }

    char line[MAX_LINE_LEN];
    while (fgets(line, sizeof(line), fp) && num_customers < max_load) {
        char *token = strtok(line, "|");
        if (token) customers[num_customers].custkey = atol(token);
        
        token = strtok(NULL, "|");
        if (token) {
            strncpy(customers[num_customers].name, token, 25);
            customers[num_customers].name[25] = '\0';
        }
        
        num_customers++;
    }

    fclose(fp);
    return 0;
}

int load_orders(const char *filename, int limit) {
    FILE *fp = fopen(filename, "r");
    if (!fp) {
        perror("orders.tbl 파일 열기 실패");
        return -1;
    }

    int max_load = (limit > 0 && limit < MAX_ORDERS) ? limit : MAX_ORDERS;
    orders = (Order *)malloc(max_load * sizeof(Order));
    if (!orders) {
        fprintf(stderr, "메모리 할당 실패\n");
        fclose(fp);
        return -1;
    }

    char line[MAX_LINE_LEN];
    while (fgets(line, sizeof(line), fp) && num_orders < max_load) {
        char *token = strtok(line, "|");
        if (token) orders[num_orders].orderkey = atol(token);
        
        token = strtok(NULL, "|");
        if (token) orders[num_orders].custkey = atol(token);
        
        token = strtok(NULL, "|");
        token = strtok(NULL, "|");
        if (token) orders[num_orders].totalprice = atof(token);
        
        num_orders++;
    }

    fclose(fp);
    return 0;
}

int load_data(int limit_customers, int limit_orders) {
    printf("데이터 로딩 중...\n");
    
    if (load_customers("../tbl/customer.tbl", limit_customers) != 0) {
        return -1;
    }
    printf("  고객 로드: %d명\n", num_customers);
    
    if (load_orders("../tbl/orders.tbl", limit_orders) != 0) {
        free(customers);
        return -1;
    }
    printf("  주문 로드: %d개\n", num_orders);
    
    return 0;
}

void cleanup_data(void) {
    free(customers);
    free(orders);
    customers = NULL;
    orders = NULL;
    num_customers = 0;
    num_orders = 0;
}
