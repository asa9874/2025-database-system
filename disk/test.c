#include <stdio.h>
#include "disk.h"

int main() {
    // TBL 파일 경로
    const char* tbl_file = "../tbl/nation.tbl";
    const char* data_file = "../bin/nation_blocks.dat";
    
    // 1. TBL 파일 읽기
    printf("=== TBL 파일 읽기 ===\n");
    RecordArray* records = parseTblFile(tbl_file);
    if (!records) {
        return 1;
    }
    printf("읽은 레코드 수: %zu\n", records->count);
    if (records->count > 0) {
        printf("첫 번째 레코드: %s|%s|%s|%s\n",
               records->records[0].nationkey,
               records->records[0].name,
               records->records[0].regionkey,
               records->records[0].comment);
    }
    
    // 2. 블록으로 저장
    printf("\n=== 블록으로 저장 ===\n");
    writeRecordsToBlocks(data_file, records);
    
    // 3. 통계 출력
    printStatistics(data_file);
    
    // 4. 블록에서 읽기
    printf("\n=== 블록에서 읽기 ===\n");
    RecordArray* loaded_records = readAllRecords(data_file);
    if (!loaded_records) {
        freeRecordArray(records);
        return 1;
    }
    printf("읽은 레코드 수: %zu\n", loaded_records->count);
    
    // 5. 검증
    printf("\n=== 검증 ===\n");
    int match = (records->count == loaded_records->count);
    if (match) {
        for (size_t i = 0; i < records->count; i++) {
            if (!compareRecords(&records->records[i], &loaded_records->records[i])) {
                match = 0;
                break;
            }
        }
    }
    
    if (match) {
        printf("✓ 모든 레코드가 올바르게 저장/복원되었습니다!\n");
    } else {
        printf("✗ 데이터 불일치!\n");
    }
    
    // 일부 레코드 출력
    printf("\n=== 복원된 레코드 샘플 ===\n");
    size_t sample_count = (loaded_records->count < 5) ? loaded_records->count : 5;
    for (size_t i = 0; i < sample_count; i++) {
        printf("%zu: %s|%s|%s|%s\n", i,
               loaded_records->records[i].nationkey,
               loaded_records->records[i].name,
               loaded_records->records[i].regionkey,
               loaded_records->records[i].comment);
    }
    
    // 메모리 해제
    freeRecordArray(records);
    freeRecordArray(loaded_records);
    
    return 0;
}
