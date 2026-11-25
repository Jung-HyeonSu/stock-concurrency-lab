// run-benchmark.sh 가 저장한 k6 요약 JSON 을 (전략, 시나리오) 별 중앙값 마크다운 표로 집계한다. jq 대신 node 사용.
//   node scripts/aggregate-results.mjs [raw_dir] > docs/results/summary.md
import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

const dir = process.argv[2] ?? 'docs/results/raw';
const files = readdirSync(dir).filter((f) => /-run\d+\.json$/.test(f));
const groups = new Map();
for (const f of files) {
  const j = JSON.parse(readFileSync(join(dir, f), 'utf8'));
  const key = `${j.strategy}|${j.scenario}|${j.variant ?? ''}|${j.rate ?? ''}`;
  if (!groups.has(key)) groups.set(key, []);
  groups.get(key).push(j);
}

const median = (xs) => {
  const a = [...xs].sort((x, y) => x - y);
  const n = a.length;
  return n % 2 ? a[(n - 1) / 2] : (a[n / 2 - 1] + a[n / 2]) / 2;
};
const fmt = (x) => x.toFixed(2);

console.log('| 전략 | 시나리오 | 회차 | TPS(req/s) | p50(ms) | p95(ms) | p99(ms) | 에러율(%) | 정합성 |');
console.log('|---|---|---|---|---|---|---|---|---|');
for (const [key, runs] of [...groups].sort()) {
  const [strategy, scenario, variant, rate] = key.split('|');
  const label = scenario + (rate ? ` @${rate}rps` : '') + (variant ? ` [${variant}]` : '');
  const okRuns = runs.filter((r) => r.verify?.consistent).length;
  const lagged = runs.filter((r) => r.verify && r.verify.pendingOrders > 0 && r.verify.mismatchedProducts === 0).length;
  const broken = runs.filter((r) => r.verify && r.verify.mismatchedProducts > 0).length;
  const verified = runs.some((r) => r.verify) ? `${okRuns}/${runs.length} 일치` + (lagged ? `, ${lagged} 처리지연(pending 잔존)` : '') + (broken ? `, ${broken} 불일치` : '') : '미검증';
  const m = (pick) => fmt(median(runs.map((r) => pick(r.summary.metrics))));
  console.log(
    `| ${strategy} | ${label} | ${runs.length} | ` +
      `${m((x) => x.http_reqs.values.rate)} | ${m((x) => x.http_req_duration.values.med)} | ` +
      `${m((x) => x.http_req_duration.values['p(95)'])} | ${m((x) => x.http_req_duration.values['p(99)'])} | ` +
      `${m((x) => x.http_req_failed.values.rate * 100)} | ${verified} |`,
  );
}
