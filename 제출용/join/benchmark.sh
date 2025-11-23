#!/bin/bash

# 블록 사이즈 벤치마크 스크립트
# 50MB부터 7000MB까지 블록 사이즈로 테스트
# 각 사이즈당 7번 실행하여 표준편차 기반 이상치 제거 후 평균 계산

echo "블록 사이즈 벤치마크 시작"
echo "========================================"

# 블록 사이즈 리스트 (MB)
block_sizes_mb=(175 200)

# 결과 파일
output_file="benchmark_results.csv"
echo "BlockSize_MB,AvgTime_sec,StdDev,ValidSamples" > "$output_file"

for size_mb in "${block_sizes_mb[@]}"; do
    echo "블록 사이즈: ${size_mb}MB 테스트 중..."
    
    # 7번 실행하여 시간 측정
    times=()
    for i in {1..7}; do
        echo "  실행 $i/7..."
        
        # 실행 시간 측정 (실제 시간만)
        start_time=$(date +%s.%N)
        ./run.out 8 $size_mb > /dev/null 2>&1
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
    
    # 통계 계산 및 이상치 제거
    if [ ${#times[@]} -lt 3 ]; then
        echo "  실행 샘플 부족 - 평균 계산 불가"
        avg_time="N/A"
        std_dev="N/A"
        valid_count=0
    else
        # 1단계: 평균 계산
        sum=0
        for t in "${times[@]}"; do
            sum=$(echo "$sum + $t" | bc)
        done
        mean=$(echo "scale=6; $sum / ${#times[@]}" | bc)
        
        # 2단계: 표준편차 계산
        sum_sq_diff=0
        for t in "${times[@]}"; do
            diff=$(echo "$t - $mean" | bc)
            sq_diff=$(echo "$diff * $diff" | bc)
            sum_sq_diff=$(echo "$sum_sq_diff + $sq_diff" | bc)
        done
        variance=$(echo "scale=6; $sum_sq_diff / ${#times[@]}" | bc)
        std_dev=$(echo "scale=6; sqrt($variance)" | bc)
        
        echo "  초기 평균: ${mean}초, 표준편차: ${std_dev}초"
        
        # 3단계: 이상치 제거 (평균 ± 2*표준편차 범위 밖의 값 제거)
        threshold=$(echo "scale=6; 2 * $std_dev" | bc)
        lower_bound=$(echo "$mean - $threshold" | bc)
        upper_bound=$(echo "$mean + $threshold" | bc)
        
        filtered_times=()
        for t in "${times[@]}"; do
            is_valid=$(echo "$t >= $lower_bound && $t <= $upper_bound" | bc)
            if [ "$is_valid" -eq 1 ]; then
                filtered_times+=($t)
            else
                echo "  이상치 제거: ${t}초"
            fi
        done
        
        # 4단계: 필터링된 데이터로 최종 평균 계산
        if [ ${#filtered_times[@]} -eq 0 ]; then
            echo "  모든 데이터가 이상치로 판단됨 - 원본 평균 사용"
            avg_time=$(echo "scale=2; $mean" | bc)
            valid_count=${#times[@]}
        else
            sum=0
            for t in "${filtered_times[@]}"; do
                sum=$(echo "$sum + $t" | bc)
            done
            avg_time=$(echo "scale=2; $sum / ${#filtered_times[@]}" | bc)
            valid_count=${#filtered_times[@]}
            echo "  최종 평균 수행 시간: ${avg_time}초 (유효 샘플: ${valid_count}/${#times[@]})"
        fi
        
        std_dev=$(echo "scale=2; $std_dev" | bc)
    fi
    
    # 결과 저장
    echo "${size_mb},${avg_time},${std_dev},${valid_count}" >> "$output_file"
    echo ""
done

echo "벤치마크 완료"
echo "결과 파일: $output_file"
echo "========================================"