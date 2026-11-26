#!/usr/bin/env bash
# 벤치마크 실행 + 결과 수집.
#   scripts/run-benchmark.sh <strategy>[,<strategy>...] <scenario>[,<scenario>...] [runs=3]
# 예:
#   scripts/run-benchmark.sh pessimistic,optimistic,redisson hot 3
#
# 공정성 규칙: 컨테이너 자원 고정(docker-compose), 워밍업 1분, 회차마다 DB/Redis 초기화(k6 setup 의 /admin/reset),
# 각 3회 실행 후 중앙값(scripts/aggregate-results.mjs).
# 필요: k6, node(검증/집계). 환경변수: RATE(기본 1000), TAG(튜닝 라벨), WARMUP(0 이면 워밍업 생략), RESULT_DIR. 앱이 BASE_URL 에서 떠 있어야 한다.
set -euo pipefail

STRATEGIES="${1:?strategy 목록을 지정하세요 (쉼표 구분)}"
SCENARIOS="${2:?scenario 목록을 지정하세요 (쉼표 구분)}"
RUNS="${3:-3}"
BASE_URL="${BASE_URL:-http://localhost:8080}"
RESULT_DIR="${RESULT_DIR:-docs/results/raw}"
WARMUP="${WARMUP:-1}"
RATE="${RATE:-1000}"
TAG="${TAG:-}"   # 튜닝 변수 라벨. 결과 파일명/집계에 variant 로 쓰인다

mkdir -p "$RESULT_DIR"

# kafka consumer lag 이 0 이 될 때까지 기다린다. 이전 회차의 잔여 메시지가 DB 초기화 후 새 회차에 섞이는 것을 막는다.
wait_kafka_drained() {
  command -v docker >/dev/null 2>&1 || return 0
  for _ in $(seq 1 90); do
    lag=$(docker exec lab-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092       --describe --group stock-consumer 2>/dev/null | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}' || true)
    [ "${lag:-0}" = "0" ] && return 0
    sleep 5
  done
  echo "경고: kafka consumer lag 이 450초 안에 0 이 되지 않았습니다 (lag=${lag:-?})"
}

for strategy in ${STRATEGIES//,/ }; do
  for scenario in ${SCENARIOS//,/ }; do
    if [ "$WARMUP" = "1" ]; then
      echo "== warmup: $strategy ($scenario) 1분"
      k6 run --quiet --log-output=none -e BASE_URL="$BASE_URL" -e STRATEGY="$strategy" -e SCENARIO=warmup \
        -e OUT="$RESULT_DIR/warmup-discard.json" k6/order.js || true
    fi
    wait_kafka_drained
    for run in $(seq 1 "$RUNS"); do
      out="$RESULT_DIR/${strategy}-${scenario}${TAG:+-$TAG}-run${run}.json"
      wait_kafka_drained
      echo "== run $run/$RUNS: $strategy / $scenario -> $out"
      # 회차 시작 시 k6 setup() 이 /admin/reset 으로 DB/Redis 를 초기화한다
      k6 run --quiet --log-output=none -e BASE_URL="$BASE_URL" -e STRATEGY="$strategy" -e SCENARIO="$scenario" \
        -e RATE="$RATE" -e TAG="$TAG" -e OUT="$out" k6/order.js
      # 부하 종료 후 DB/Redis 최종 정합성 자동 검증 (kafka 는 Consumer 처리 완료까지 대기)
      node scripts/verify-run.mjs "$out" "$BASE_URL"
      # 다음 회차 전에 Kafka consumer 가 남은 메시지를 소진할 시간을 준다
      sleep 3
    done
  done
done

echo "완료. 집계: node scripts/aggregate-results.mjs $RESULT_DIR"
