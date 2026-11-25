# stock-concurrency-lab

한정판 상품 선착순 구매라는 하나의 요구사항을 여러 동시성 제어 전략으로 구현하고, 정합성과 성능을 같은 조건에서 측정·비교하는 프로젝트.

## 요구사항

- 재고 N개 상품에 동시 요청 M건이 몰리는 상황 (예: 재고 100, 요청 10,000)
- 정합성 조건 (모든 전략이 반드시 만족)
  - 재고는 음수가 될 수 없다
  - 성공 주문 수 == 차감된 재고 수
  - 1인 1회 구매 (product_id + user_id 유니크)
- 비교 지표: TPS, p50/p95/p99 latency, 에러율, 정합성, 자원 사용량

## 기술 스택

- Java 21, Spring Boot 3.x (최신 안정 버전), Gradle (Kotlin DSL)
- Spring Data JPA, MySQL 8
- Redis 7 + Redisson
- Kafka (KRaft 모드, ZooKeeper 없음)
- 테스트: JUnit5, Testcontainers (MySQL, Redis, Kafka)
- 부하 테스트: k6
- 모니터링: Micrometer → Prometheus → Grafana
- 실행 환경: Docker Compose (컨테이너별 CPU/메모리 제한 명시)

## 패키지 구조

```
src/main/java/.../
├── domain/        # Product, Order, OrderStatus, Repository
├── strategy/      # StockStrategy 인터페이스 + 구현체
├── facade/        # 트랜잭션 바깥에서 재시도/락을 감싸는 Facade
├── api/           # Controller, DTO, 예외 핸들러
├── consumer/      # Kafka Consumer
└── config/        # Redis, Kafka, 메트릭 설정
k6/                # 부하 시나리오 스크립트
docker/            # docker-compose.yml, prometheus.yml, grafana 대시보드 JSON
docs/              # 벤치마크 결과 리포트
```

## ERD

```
product: id PK, name, stock INT NOT NULL, version BIGINT
orders:  id PK, product_id, user_id, status(PENDING/SUCCESS/SOLD_OUT), created_at
         UNIQUE(product_id, user_id)
```

유니크 제약은 어떤 전략에서든 중복 주문의 최후 방어선이다.

## 공통 인터페이스

```java
public interface StockStrategy {
    OrderResult order(Long productId, Long userId);
    String name();
}
```

- API: `POST /api/orders?strategy={name}` body `{productId, userId}`
- 전략은 이름으로 Bean을 찾아 주입 (Map<String, StockStrategy>)
- Kafka 전략은 202 Accepted + orderId 반환, `GET /api/orders/{id}`로 상태 조회

## 전략 명세

### 0. no-lock (베이스라인)
- findById → stock 감소 → save. Lost Update가 발생함을 테스트로 증명한다 (이 테스트는 "실패함을 검증"하는 테스트로 작성).

### 1. pessimistic
- `@Lock(PESSIMISTIC_WRITE)` + lock timeout 힌트 3000ms
- HikariCP pool 크기를 설정값으로 분리 (벤치마크에서 변경하며 측정)

### 2. optimistic
- `@Version` 사용, 충돌 시 재시도
- 재시도는 반드시 트랜잭션 바깥 Facade에서 수행
- 지수 백오프 + jitter, 최대 재시도 횟수 설정값
- 재시도 횟수를 Micrometer Counter로 노출

### 3. redisson
- 키: `lock:product:{id}`, tryLock(waitTime, leaseTime) 설정값
- 순서: 락 획득 → 트랜잭션 시작 → 커밋 → 락 해제 (Facade에서 락, 내부 서비스에서 @Transactional)
- 비교용으로 `redisson-wrong` 전략도 구현: @Transactional 메서드 안에서 락을 해제해 커밋 전 락이 풀리는 버그 재현. 테스트로 정합성이 깨질 수 있음을 보여준다.

### 4. redis-atomic (락 없는 방식)
- 재고를 Redis에 적재, Lua 스크립트 하나로 [중복 체크(SADD) → 재고 확인 → DECR]을 원자 처리
- 성공 시 DB에 주문 저장. DB 저장 실패 시 Redis 재고 복구(INCR + SREM) 보상 처리
- 재고 초기화/동기화용 API 또는 시작 시 로더 제공

### 5. kafka
- API: orders에 PENDING 저장 → Kafka publish (key=productId) → 202 반환
- 토픽 파티션 여러 개, 같은 productId는 같은 파티션 → 순차 처리로 락 불필요
- Consumer: 재고 차감 후 SUCCESS / SOLD_OUT으로 상태 갱신
- 멱등성: 이미 처리된 주문(status != PENDING)은 skip
- 지표: API 응답시간과 별도로 end-to-end(요청 → 확정) 시간을 Timer로 기록

## 테스트

- Testcontainers 기반 통합 테스트
- 모든 전략에 동일한 정합성 테스트를 `@ParameterizedTest`로 적용
  - 재고 100, 서로 다른 사용자 1000명 동시 요청 (ExecutorService + CountDownLatch)
  - 검증: 최종 재고 0, SUCCESS 주문 수 100, 중복 user 없음
  - Kafka는 Awaitility로 처리 완료 대기
- 동일 사용자 중복 요청 테스트
- no-lock, redisson-wrong은 "정합성이 깨짐"을 검증

## 벤치마크

- k6 시나리오 (ramping-arrival-rate)
  - A. 단일 핫 상품 (최대 경합)
  - B. 상품 100개 랜덤 분산 (저경합)
  - C. 재고:요청 비율 1:10 / 1:100
  - D. 부하 단계 VU 100 → 500 → 1000
- 전략은 환경변수로 선택: `k6 run -e STRATEGY=pessimistic -e SCENARIO=hot k6/order.js`
- 공정성 규칙: 컨테이너 자원 고정, 워밍업 1분, 각 3회 실행 후 중앙값, 회차마다 DB/Redis 초기화
- 초기화용 스크립트 또는 관리 API (`POST /admin/reset?stock=100&products=1`)
- 결과 자동 수집: k6 summary를 JSON으로 저장하는 `scripts/run-benchmark.sh`
- Grafana 대시보드: TPS, latency, Hikari active/pending, 재시도 횟수, Redis ops, Kafka consumer lag

## README 구성

1. 프로젝트 소개, 한 줄 요약
2. 문제 정의: no-lock에서 재고가 깨지는 결과
3. 아키텍처 다이어그램 (Mermaid)
4. 전략별 구현 요약 + 주의할 함정 (트랜잭션과 락 순서, 재시도 위치, 커넥션 풀 고갈 등)
5. 실행 방법 (docker compose up, 테스트, 벤치마크)
6. 측정 환경과 방법론
7. 결과 표/그래프 — 실측 전에는 "측정 예정" 자리표시자로 두고, 수치를 지어내지 않는다
8. 결론: 상황별로 어떤 전략을 쓸지
9. 트러블슈팅 기록

## 작업 규칙

- 단계별로 진행하고, 각 단계가 끝나면 빌드와 테스트를 통과시킨 뒤 커밋한다 (Conventional Commits, 한국어 메시지 가능)
  1. 프로젝트 셋업 + 도메인 + docker-compose(MySQL, Redis, Kafka)
  2. no-lock, pessimistic, optimistic + 정합성 테스트
  3. redisson, redisson-wrong, redis-atomic + 테스트
  4. kafka 전략 + 상태 조회 + 테스트
  5. 메트릭, Prometheus, Grafana, k6 스크립트, 벤치마크 스크립트
  6. README, docs
- 설정값(타임아웃, 재시도, 풀 크기, 락 시간)은 application.yml로 분리
- 테스트가 실패하면 테스트를 약화시키지 말고 원인을 고친다
- 벤치마크 수치는 실제로 실행한 결과만 기록한다
- 코드 주석과 문서는 한국어, 식별자는 영어
