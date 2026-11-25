#!/usr/bin/env bash
# run-benchmark.sh 가 저장한 k6 요약 JSON 을 (전략, 시나리오) 별 중앙값 표(마크다운)로 집계한다. 필요: jq
#   scripts/aggregate-results.sh [raw_dir] > docs/results/summary.md
set -euo pipefail

DIR="${1:-docs/results/raw}"

median() { sort -g | awk '{a[NR]=$1} END {if (NR==0) print "NA"; else if (NR%2) printf "%.2f", a[(NR+1)/2]; else printf "%.2f", (a[NR/2]+a[NR/2+1])/2}'; }

echo "| 전략 | 시나리오 | 회차 | TPS(req/s) | p50(ms) | p95(ms) | p99(ms) | 에러율(%) |"
echo "|---|---|---|---|---|---|---|---|"

for key in $(ls "$DIR"/*-run*.json 2>/dev/null | sed -E 's/-run[0-9]+\.json$//' | sort -u); do
  files=$(ls "${key}"-run*.json)
  n=$(echo "$files" | wc -l | tr -d ' ')
  q() { for f in $files; do jq -r "$1" "$f"; done | median; }
  strategy=$(jq -r '.strategy' $(echo "$files" | head -1))
  scenario=$(jq -r '.scenario' $(echo "$files" | head -1))
  tps=$(q '.summary.metrics.http_reqs.values.rate')
  p50=$(q '.summary.metrics.http_req_duration.values.med')
  p95=$(q '.summary.metrics.http_req_duration.values["p(95)"]')
  p99=$(q '.summary.metrics.http_req_duration.values["p(99)"]')
  err=$(q '(.summary.metrics.http_req_failed.values.rate * 100)')
  echo "| $strategy | $scenario | $n | $tps | $p50 | $p95 | $p99 | $err |"
done
