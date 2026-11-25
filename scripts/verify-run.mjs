// 부하 실행 직후 /admin/verify 로 DB/Redis 최종 정합성을 검증하고 결과 JSON 에 verify 필드로 덧붙인다.
//   node scripts/verify-run.mjs <result.json> [baseUrl]
// kafka 전략은 Consumer 가 PENDING 을 모두 처리할 때까지(최대 180초) 폴링한다.
import { readFileSync, writeFileSync } from 'node:fs';

const file = process.argv[2];
const base = process.argv[3] ?? 'http://localhost:8080';
const result = JSON.parse(readFileSync(file, 'utf8'));
const deadline = Date.now() + 180_000;

let verify;
for (;;) {
  const res = await fetch(`${base}/admin/verify?initialStock=${result.stock}`);
  verify = await res.json();
  if (verify.pendingOrders === 0 || Date.now() > deadline) break;
  await new Promise((r) => setTimeout(r, 2000));
}
result.verify = verify;
writeFileSync(file, JSON.stringify(result, null, 2));
console.log(`verify ${result.strategy}/${result.scenario}: consistent=${verify.consistent} success=${verify.successOrders} pending=${verify.pendingOrders} mismatched=${verify.mismatchedProducts} negative=${verify.negativeStockProducts}`);
