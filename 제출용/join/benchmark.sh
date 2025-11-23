#!/bin/bash

# 블록 사이즈 벤치마크 스크립트
# 50MB부터 7000MB까지 블록 사이즈로 테스트
# 각 사이즈당 5번 실행하여 평균 수행 시간 계산

echo "블록 사이즈 벤치마크 시작"
echo "========================================"

# 블록 사이즈 리스트 (MB)
block_sizes_mb=(50 100 200 500 1000 2000 5000 7000)

# 결과 파일
output_file="benchmark_results.csv"
echo "BlockSize_MB,AvgTime_sec" > "$output_file"

for size_mb in "${block_sizes_mb[@]}"; do
    echo "블록 사이즈: ${size_mb}MB 테스트 중..."
    
    # 바이트로 변환
    size_bytes=$((size_mb * 1024 * 1024))
    
    # 5번 실행하여 시간 측정
    times=()
    for i in {1..5}; do
        echo "  실행 $i/5..."
        
        # 실행 시간 측정 (실제 시간만)
        start_time=$(date +%s.%N)
        ./test_flexible 4 $size_bytes > /dev/null 2>&1
        exit_code=$?
        end_time=$(date +%s.%N)
        
        if [ $exit_code -ne 0 ]; then
            echo "  실행 실패 (종료 코드: $exit_code)"
            continue
        fi
        
        # 시간 계산
        elapsed=$(echo "$end_time - $start_time" | bc)
        times+=($elapsed)
        echo "  수행 시간: ${elapsed}초"
    done
    
    # 평균 계산
    if [ ${#times[@]} -eq 0 ]; then
        echo "  모든 실행 실패 - 평균 계산 불가"
        avg_time="N/A"
    else
        sum=0
        for t in "${times[@]}"; do
            sum=$(echo "$sum + $t" | bc)
        done
        avg_time=$(echo "scale=2; $sum / ${#times[@]}" | bc)
        echo "  평균 수행 시간: ${avg_time}초"
    fi
    
    # 결과 저장
    echo "${size_mb},${avg_time}" >> "$output_file"
    echo ""
done

echo "벤치마크 완료"
echo "결과 파일: $output_file"
echo "========================================"