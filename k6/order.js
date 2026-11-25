// 선착순 주문 부하 시나리오 (ramping-arrival-rate)
//
// 실행 예:
//   k6 run -e STRATEGY=pessimistic -e SCENARIO=hot k6/order.js
//
// 환경변수
//   STRATEGY  전략 이름 (no-lock | pessimistic | optimistic | redisson | redisson-wrong | redis-atomic | kafka)
//   SCENARIO  hot | spread | ratio10 | ratio100 | stages | warmup
//   BASE_URL  기본 http://localhost:8080
//   RATE      최대 도착률(req/s), 기본 1000
//   OUT       요약 JSON 저장 경로 (기본 k6/results/<STRATEGY>-<SCENARIO>.json)
import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const STRATEGY = __ENV.STRATEGY || 'pessimistic';
const SCENARIO = __ENV.SCENARIO || 'hot';
const RATE = parseInt(__ENV.RATE || '1000');
const TAG = __ENV.TAG || ''; // 튜닝 변수 라벨 (예: hikari50). 결과 JSON 의 variant 로 기록된다
const OUT = __ENV.OUT || `k6/results/${STRATEGY}-${SCENARIO}.json`;

// 5xx / 네트워크 오류만 실패로 센다. 200(성공/품절), 202(kafka 접수), 409(중복)는 정상 응답.
http.setResponseCallback(http.expectedStatuses(200, 202, 409));

const orderOutcome = {
  success: new Counter('orders_success'),
  accepted: new Counter('orders_accepted'),
  duplicate: new Counter('orders_duplicate'),
};

// 단계별 [도착률 목표, 지속시간(초)]
function plan() {
  switch (SCENARIO) {
    case 'warmup':
      return { stages: [{ target: Math.max(50, RATE / 5), duration: '60s' }], products: 1, stock: 100, ratio: null };
    case 'hot': // A. 단일 핫 상품 (최대 경합)
      return { stages: ramp(), products: 1, stock: 100, ratio: null };
    case 'spread': // B. 상품 100개 랜덤 분산 (저경합)
      return { stages: ramp(), products: 100, stock: 100, ratio: null };
    case 'ratio10': // C. 재고:요청 = 1:10
      return { stages: ramp(), products: 1, stock: null, ratio: 10 };
    case 'ratio100': // C. 재고:요청 = 1:100
      return { stages: ramp(), products: 1, stock: null, ratio: 100 };
    case 'stages': // D. 부하 단계 100 → 500 → 1000 (도착률 기준, 단계마다 30초 유지)
      return {
        stages: [
          { target: 100, duration: '10s' }, { target: 100, duration: '30s' },
          { target: 500, duration: '10s' }, { target: 500, duration: '30s' },
          { target: 1000, duration: '10s' }, { target: 1000, duration: '30s' },
        ],
        products: 1, stock: 100, ratio: null,
      };
    default:
      throw new Error(`unknown SCENARIO: ${SCENARIO}`);
  }
}

function ramp() {
  return [
    { target: RATE, duration: '15s' },
    { target: RATE, duration: '30s' },
  ];
}

// 램프 구간을 선형 보간해 예상 총 요청 수를 구한다 (ratio 시나리오의 재고 계산용)
function expectedRequests(stages) {
  let prev = 0;
  let total = 0;
  for (const s of stages) {
    const sec = parseInt(s.duration);
    total += ((prev + s.target) / 2) * sec;
    prev = s.target;
  }
  return Math.ceil(total);
}

const P = plan();
const STOCK = P.ratio ? Math.max(1, Math.ceil(expectedRequests(P.stages) / P.ratio)) : P.stock;

export const options = {
  scenarios: {
    orders: {
      executor: 'ramping-arrival-rate',
      startRate: 10,
      timeUnit: '1s',
      preAllocatedVUs: 200,
      maxVUs: 2000,
      stages: P.stages,
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

// 회차마다 DB/Redis 를 같은 초기 상태로 되돌린다.
export function setup() {
  const res = http.post(`${BASE_URL}/admin/reset?stock=${STOCK}&products=${P.products}`);
  if (res.status !== 200) {
    throw new Error(`reset 실패: ${res.status} ${res.body}`);
  }
  return { stock: STOCK, products: P.products };
}

export default function () {
  // iterationInTest 는 전체 테스트에서 유일하므로 사용자 중복 없이 "서로 다른 사용자"가 된다
  const userId = exec.scenario.iterationInTest + 1;
  const productId = P.products === 1 ? 1 : 1 + Math.floor(Math.random() * P.products);

  const res = http.post(
    `${BASE_URL}/api/orders?strategy=${STRATEGY}`,
    JSON.stringify({ productId, userId }),
    { headers: { 'Content-Type': 'application/json' }, tags: { strategy: STRATEGY, scenario: SCENARIO } },
  );

  check(res, { 'not 5xx': (r) => r.status < 500 });
  if (res.status === 200) orderOutcome.success.add(1);
  else if (res.status === 202) orderOutcome.accepted.add(1);
  else if (res.status === 409) orderOutcome.duplicate.add(1);
}

// 결과 자동 수집: k6 summary 를 JSON 으로 저장한다.
export function handleSummary(data) {
  return {
    [OUT]: JSON.stringify(
      { strategy: STRATEGY, scenario: SCENARIO, variant: TAG, rate: RATE, stock: STOCK, products: P.products, summary: data },
      null,
      2,
    ),
    stdout: `\n${STRATEGY}/${SCENARIO}: ` +
      `reqs=${data.metrics.http_reqs.values.count} ` +
      `rate=${data.metrics.http_reqs.values.rate.toFixed(1)}/s ` +
      `p95=${data.metrics.http_req_duration.values['p(95)'].toFixed(1)}ms ` +
      `failed=${(data.metrics.http_req_failed.values.rate * 100).toFixed(2)}% -> ${OUT}\n`,
  };
}
