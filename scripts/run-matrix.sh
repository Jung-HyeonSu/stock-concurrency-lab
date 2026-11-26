#!/usr/bin/env bash
# 전략 x 시나리오 매트릭스 실행. 전략이 바뀔 때마다 앱 컨테이너를 재기동해 JVM/커넥션 상태가 섞이지 않게 한다.
#   scripts/run-matrix.sh <strategies> <scenarios> [runs=3]
# 예: scripts/run-matrix.sh pessimistic,optimistic,redisson,redis-atomic,kafka spread,ratio10,ratio100,stages 3
# 워밍업(1분)은 전략마다 첫 시나리오에서 한 번만 한다. 환경변수는 run-benchmark.sh 와 같다(RATE, TAG, RESULT_DIR ...).
# 주의: 실행 중에 이 스크립트나 run-benchmark.sh 를 수정하지 말 것 (bash 가 파일을 읽으며 실행한다).
set -euo pipefail

STRATEGIES="${1:?strategy 목록}"
SCENARIOS="${2:?scenario 목록}"
RUNS="${3:-3}"
BASE_URL="${BASE_URL:-http://localhost:8080}"
COMPOSE="${COMPOSE:-docker compose -f docker/docker-compose.yml}"

restart_app() {
  $COMPOSE up -d --force-recreate app >/dev/null 2>&1
  for _ in $(seq 1 60); do
    if curl -fs "$BASE_URL/actuator/health" 2>/dev/null | grep -q '"UP"'; then return 0; fi
    sleep 3
  done
  echo "앱이 기동하지 않았습니다" >&2
  return 1
}

for strategy in ${STRATEGIES//,/ }; do
  echo "##### strategy=$strategy: 앱 재기동"
  restart_app
  first=1
  for scenario in ${SCENARIOS//,/ }; do
    if [ "$first" = "1" ]; then warm=1; else warm=0; fi
    WARMUP=$warm scripts/run-benchmark.sh "$strategy" "$scenario" "$RUNS"
    first=0
  done
done
echo "매트릭스 완료"
