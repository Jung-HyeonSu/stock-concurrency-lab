#!/usr/bin/env bash
# 벤치마크 실행 + 결과 수집.
#   scripts/run-benchmark.sh <strategy>[,<strategy>...] <scenario>[,<scenario>...] [runs=3]
# 예:
#   scripts/run-benchmark.sh pessimistic,optimistic,redisson hot 3
#
# 공정성 규칙: 컨테이너 자원 고정(docker-compose), 워밍업 1분, 회차마다 DB/Redis 초기화(k6 setup 의 /admin/reset),
# 각 3회 실행 후 중앙값(scripts/aggregate-results.sh).
# 필요: k6, curl, (집계 시) jq. 앱이 BASE_URL 에서 떠 있어야 한다.
set -euo pipefail

STRATEGIES="${1:?strategy 목록을 지정하세요 (쉼표 구분)}"
SCENARIOS="${2:?scenario 목록을 지정하세요 (쉼표 구분)}"
RUNS="${3:-3}"
BASE_URL="${BASE_URL:-http://localhost:8080}"
RESULT_DIR="${RESULT_DIR:-docs/results/raw}"
WARMUP="${WARMUP:-1}"

mkdir -p "$RESULT_DIR"

for strategy in ${STRATEGIES//,/ }; do
  for scenario in ${SCENARIOS//,/ }; do
    if [ "$WARMUP" = "1" ]; then
      echo "== warmup: $strategy ($scenario) 1분"
      k6 run --quiet -e BASE_URL="$BASE_URL" -e STRATEGY="$strategy" -e SCENARIO=warmup \
        -e OUT="$RESULT_DIR/warmup-discard.json" k6/order.js || true
    fi
    for run in $(seq 1 "$RUNS"); do
      out="$RESULT_DIR/${strategy}-${scenario}-run${run}.json"
      echo "== run $run/$RUNS: $strategy / $scenario -> $out"
      # 회차 시작 시 k6 setup() 이 /admin/reset 으로 DB/Redis 를 초기화한다
      k6 run --quiet -e BASE_URL="$BASE_URL" -e STRATEGY="$strategy" -e SCENARIO="$scenario" \
        -e OUT="$out" k6/order.js
      # 다음 회차 전에 Kafka consumer 가 남은 메시지를 소진할 시간을 준다
      sleep 5
    done
  done
done

echo "완료. 집계: scripts/aggregate-results.sh $RESULT_DIR"
