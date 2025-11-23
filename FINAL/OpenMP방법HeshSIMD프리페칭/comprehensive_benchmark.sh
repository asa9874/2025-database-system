#!/bin/bash

# 종합 벤치마크 스크립트: 스레드 수별 성능 비교 (1, 2, 4, 8 스레드, 각 10회 반복)

echo "=================================================================="
echo "SIMD 최적화 OpenMP 병렬 JOIN 종합 성능 벤치마크"
echo "=================================================================="
echo "테스트 조건:"
echo "  - 스레드 수: 1, 2, 3, 4, 5, 6, 7, 8"
echo "  - 반복 횟수: 각 스레드 수당 10회"
echo "  - 데이터: Customer 150,000개, Order 전체"
echo "  - 버퍼 블록: 100"
echo "  - 최적화: SIMD 자동 벡터화 적용"
echo ""

# 결과를 저장할 배열
declare -a thread_counts=(1 2 3 4 5 6 7 8)
declare -a avg_times
declare -a success_counts

cd OpenMP방법

for num_threads in "${thread_counts[@]}"; do
    echo "=================================================================="
    echo "스레드 수: $num_threads 개 테스트 중..."
    echo "=================================================================="
    
    times=()
    success_count=0
    
    for i in {1..10}; do
        echo -n "실행 $i/10: "
        
        # 타임아웃 180초로 설정 (더 긴 실행 고려)
        output=$(timeout 180 ./test_flexible $num_threads 2>/dev/null | grep "실행 시간:" | sed 's/.*실행 시간: \([0-9.]*\)초.*/\1/')
        
        if [ -n "$output" ] && [ "$output" != "0" ]; then
            times+=($output)
            ((success_count++))
            echo "${output}초"
        else
            echo "실패 또는 타임아웃"
            times+=(0)
        fi
    done
    
    # 평균 계산
    sum=0
    valid_count=0
    for time in "${times[@]}"; do
        if (( $(echo "$time > 0" | bc -l) )); then
            sum=$(echo "$sum + $time" | bc -l)
            ((valid_count++))
        fi
    done
    
    if [ $valid_count -gt 0 ]; then
        avg=$(echo "scale=3; $sum / $valid_count" | bc -l)
        avg_times+=($avg)
        success_counts+=($success_count)
        
        echo ""
        echo "스레드 $num_threads 개 결과:"
        echo "  - 평균 실행 시간: ${avg}초"
        echo "  - 성공 실행 횟수: $success_count/10"
        echo "  - 상세 시간: ${times[*]}"
    else
        avg_times+=(0)
        success_counts+=(0)
        echo "스레드 $num_threads 개: 모든 실행 실패"
    fi
    
    echo ""
done

cd ..

# 최종 결과 비교
echo "=================================================================="
echo "최종 성능 비교 결과"
echo "=================================================================="

echo "스레드 수 | 평균 시간 | 성공률 | 속도 향상"
echo "----------|-----------|--------|----------"

baseline=${avg_times[0]}
for i in "${!thread_counts[@]}"; do
    threads=${thread_counts[$i]}
    avg_time=${avg_times[$i]}
    success=${success_counts[$i]}
    
    if (( $(echo "$baseline > 0" | bc -l) )) && (( $(echo "$avg_time > 0" | bc -l) )); then
        speedup=$(echo "scale=2; $baseline / $avg_time" | bc -l)
        efficiency=$(echo "scale=1; $speedup / $threads * 100" | bc -l)
    else
        speedup="N/A"
        efficiency="N/A"
    fi
    
    printf "%-9s | %-9s | %2d/10  | %s\n" "$threads" "${avg_time}초" "$success" "$speedup배"
done

echo ""
echo "분석:"
echo "- 기준(1스레드) 대비 속도 향상 배수"
echo "- 이상적인 경우: 스레드 수만큼 선형 향상"
echo "- 실제로는 I/O 중복, 동기화 등으로 효율성 저하"

# 추가 통계
echo ""
echo "추가 통계:"
valid_results=0
for time in "${avg_times[@]}"; do
    if (( $(echo "$time > 0" | bc -l) )); then
        ((valid_results++))
    fi
done

if [ $valid_results -gt 1 ]; then
    min_time=${avg_times[0]}
    max_time=${avg_times[0]}
    for time in "${avg_times[@]}"; do
        if (( $(echo "$time > $max_time" | bc -l) )); then
            max_time=$time
        fi
        if (( $(echo "$time < $min_time && $time > 0" | bc -l) )); then
            min_time=$time
        fi
    done
    
    echo "- 최고 성능: ${min_time}초"
    echo "- 최저 성능: ${max_time}초"
    echo "- 성능 범위: $(echo "scale=2; $max_time - $min_time" | bc -l)초"
fi

echo ""
echo "=================================================================="
echo "벤치마크 완료"
echo "=================================================================="
