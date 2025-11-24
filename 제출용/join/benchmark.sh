#!/bin/bash

# 블록 사이즈 벤치마크 스크립트
# 50MB부터 7000MB까지 블록 사이즈로 테스트
# 각 사이즈당 7번 실행하여 표준편차 기반 이상치 제거 후 평균 계산

echo "블록 사이즈 벤치마크 시작"
echo "========================================"

# 블록 사이즈 리스트 (MB)
block_sizes_mb=(193 197)

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
    
    # 통계 계산 및 이상치 제거 (중간값 기준)
    if [ ${#times[@]} -lt 3 ]; then
        echo "  실행 샘플 부족 - 평균 계산 불가"
        avg_time="N/A"
        std_dev="N/A"
        valid_count=0
    else
        # 1단계: 데이터 정렬
        IFS=$'\n' sorted_times=($(sort -n <<<"${times[*]}"))
        unset IFS
        
        # 2단계: 중간값(Median) 계산
        count=${#sorted_times[@]}
        mid=$((count / 2))
        if [ $((count % 2)) -eq 1 ]; then
            # 홀수 개: 중간값
            median=${sorted_times[$mid]}
        else
            # 짝수 개: 중간 두 값의 평균
            median=$(echo "scale=6; (${sorted_times[$mid-1]} + ${sorted_times[$mid]}) / 2" | bc)
        fi
        
        echo "  정렬된 데이터: ${sorted_times[*]}"
        echo "  중간값: ${median}초"
        
        # 3단계: MAD (Median Absolute Deviation) 계산
        abs_devs=()
        for t in "${times[@]}"; do
            abs_dev=$(echo "scale=6; if ($t - $median >= 0) $t - $median else $median - $t" | bc)
            abs_devs+=($abs_dev)
        done
        IFS=$'\n' sorted_abs_devs=($(sort -n <<<"${abs_devs[*]}"))
        unset IFS
        
        mad_mid=$((${#sorted_abs_devs[@]} / 2))
        if [ $((${#sorted_abs_devs[@]} % 2)) -eq 1 ]; then
            mad=${sorted_abs_devs[$mad_mid]}
        else
            mad=$(echo "scale=6; (${sorted_abs_devs[$mad_mid-1]} + ${sorted_abs_devs[$mad_mid]}) / 2" | bc)
        fi
        
        # 4단계: 이상치 제거 (중간값 ± 2.5 * MAD 범위)
        # MAD를 표준편차로 변환: std ≈ 1.4826 * MAD
        std_dev=$(echo "scale=6; 1.4826 * $mad" | bc)
        threshold=$(echo "scale=6; 2.5 * $std_dev" | bc)
        lower_bound=$(echo "$median - $threshold" | bc)
        upper_bound=$(echo "$median + $threshold" | bc)
        
        echo "  표준편차(추정): ${std_dev}초"
        echo "  유효 범위: [${lower_bound}, ${upper_bound}]초"
        
        filtered_times=()
        for t in "${times[@]}"; do
            is_valid=$(echo "$t >= $lower_bound && $t <= $upper_bound" | bc)
            if [ "$is_valid" -eq 1 ]; then
                filtered_times+=($t)
            else
                echo "  이상치 제거: ${t}초"
            fi
        done
        
        # 5단계: 필터링된 데이터로 최종 평균 계산
        if [ ${#filtered_times[@]} -eq 0 ]; then
            echo "  모든 데이터가 이상치로 판단됨 - 중간값 사용"
            avg_time=$(echo "scale=2; $median" | bc)
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