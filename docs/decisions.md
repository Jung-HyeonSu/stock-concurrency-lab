# 설계 결정 기록

CLAUDE.md 설계에서 애매하거나 바꿔야 했던 부분과 그 이유를 기록한다.

## D-01. Gradle 래퍼 생성 방식
- 개발 PC에 `gradle` 이 설치돼 있지 않았다. `~/.gradle/wrapper/dists` 에 캐시된 Gradle 8.14.3 으로 `gradle wrapper` 를 실행해 래퍼를 생성했다.
- Spring Boot 는 3.x 최신 안정 라인인 3.5.7 을 사용한다.

## D-02. Product 엔티티를 둘로 분리 (Product / VersionedProduct)
- ERD 의 `product.version` 컬럼은 유지하되, `@Version` 이 붙은 엔티티로 no-lock 을 구현하면 Lost Update 대신 `OptimisticLockException` 이 발생해 "락 없이 재고가 깨진다"는 베이스라인을 증명할 수 없다.
- 같은 `product` 테이블을 `@Version` 없는 `Product`(no-lock, pessimistic, redisson, kafka)와 `@Version` 있는 `VersionedProduct`(optimistic)로 매핑했다. 한 번의 실행에서는 한 전략만 쓰고, 회차마다 DB를 초기화하므로 충돌하지 않는다.

## D-03. 스키마는 schema.sql 로 관리 (ddl-auto: none)
- 유니크 제약 등 ERD 를 그대로 고정하고 MySQL/H2 양쪽에서 동일하게 동작시키기 위해 `schema.sql` 을 쓴다 (`CREATE TABLE IF NOT EXISTS`).

## D-04. 한글 사용자 경로에서의 빌드 (Windows)
- 이 개발 PC는 사용자 홈과 프로젝트 경로에 한글이 포함돼 있어 Gradle 테스트 워커가 `ClassNotFoundException` 으로 죽는다 (Gradle 워커가 JVM 기본 인코딩으로 클래스패스를 전달하는 문제).
- 해결: ASCII 경로를 쓴다. `GRADLE_USER_HOME` 을 `C:/gradle-home` 으로, 빌드 출력은 `build.gradle.kts` 가 읽는 `LAB_BUILD_DIR=C:/gradle-build/stock-lab` 으로 지정한다. 경로가 ASCII 인 환경(Linux/macOS/CI)에서는 아무 설정도 필요 없다.

## D-05. Docker 가 없는 환경의 테스트 전략
- 개발 PC에 Docker 가 없어 Testcontainers 테스트를 실행·검증할 수 없었다. 그래서 테스트를 두 층으로 나눴다.
  1. `H2StrategyConsistencyTest` 등: H2(MySQL 모드) + 임베디드 Kafka. Docker 없이 항상 실행되며 `./gradlew build` 에 포함된다. Redis 가 필요한 전략은 제외된다.
  2. `ContainerStrategyConsistencyTest`: Testcontainers(MySQL/Redis/Kafka)로 CLAUDE.md 명세 그대로 모든 전략을 검증한다. `@Testcontainers(disabledWithoutDocker = true)` 이므로 Docker 가 없으면 **건너뛴다(SKIPPED)**. 테스트를 약화하거나 `@Disabled` 로 끈 것이 아니라 Docker 유무에 따른 자동 선택이다. Docker 가 있는 환경에서는 같은 `./gradlew build` 가 이 테스트까지 실행한다.
- 2번 층은 이 환경에서 **실행해 보지 못했다**. README/최종 보고에 그대로 명시한다.

## D-06. HTTP 상태 코드 규약
- 성공/품절: 200 (품절은 `status=SOLD_OUT`, orderId=null). kafka 접수: 202. 중복 구매(UNIQUE 위반): 409. 락 대기 초과/재시도 초과/Redis 미초기화: 503. 알 수 없는 전략: 400.
- 벤치마크의 "에러율"은 5xx 와 네트워크 오류만 센다. 품절·중복은 정상 비즈니스 응답이다.

## D-07. no-lock 의 인위적 지연
- 로컬 H2/MySQL 에서 Lost Update 는 확률적이다. 테스트에서 재현 확률을 높이기 위해 `app.simulation.no-lock-think-time-ms` (기본 0) 로 읽기와 쓰기 사이에 지연을 줄 수 있게 했다. 테스트만 5ms 로 설정하고 벤치마크는 0 으로 둔다.

## D-08. kafka 전략: publish 는 브로커 ack 를 기다린다
- PENDING 저장을 커밋한 뒤 `send().get(5s)` 로 ack 를 기다리고 202 를 반환한다. 비동기로 던지면 API 지연은 줄지만 publish 실패 시 PENDING 주문이 영원히 남는다. 접수 신뢰성을 택했고, 이 지연은 API latency 에 포함된다.
- 메시지는 JSON 문자열(Jackson)이며 key=productId. Consumer 는 DB 초기화 이전의 잔여 메시지(주문 없음)는 경고 후 건너뛴다.
- 임베디드 Kafka(KRaft)로 H2 와 함께 Docker 없이 검증했다 (`H2KafkaStrategyTest`).

## D-09. Testcontainers 실행 환경 보정 (Docker Desktop 29, Windows)
- Docker 29 는 최소 API 1.40 이라 Testcontainers 1.21.3 의 기본 API 버전을 거부한다. `~/.docker-java.properties` 에 `api.version=1.44` 를 두면 인식된다 (프로젝트 코드 변경 없음, 개발 PC 설정).
- Testcontainers 1.21.3 의 `KafkaContainer` 는 `apache/kafka:3.9.0` 에서 `advertised.listeners ... 0.0.0.0` 로 기동에 실패해 테스트는 `apache/kafka:3.8.0` 을 쓴다. compose 는 리스너를 직접 지정하므로 3.9.0 을 유지한다.

## D-10. redisson 전략: 핫 상품에 100 스레드가 몰리면 waitTime 3s 초과 (실측 관찰)
- MySQL/Redis 컨테이너에서 재고 100, 1000 요청, 100 스레드 동시 실행 시 기본 `wait-time-ms=3000` 에서 약 10~20% 요청이 `LockAcquisitionException`(503)으로 실패했다 (두 번 실행: 96건, 218건 / 1000건). 정합성 위반이 아니라 락 대기 한도 초과다.
- 정합성 테스트는 "정합성이 유지되는가"를 보는 것이므로 테스트에서만 `app.redisson.wait-time-ms=60000` 으로 올렸다. 운영/벤치마크 기본값은 3000 으로 두고, 이 값 자체가 redisson 의 에러율을 좌우한다는 점을 벤치마크에서 측정 대상으로 삼는다.
