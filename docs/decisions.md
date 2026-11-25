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
