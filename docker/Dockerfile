# 사용 전: ./gradlew bootJar  (build/libs/app.jar 생성)
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY build/libs/app.jar app.jar
ENV JAVA_OPTS="-Xms512m -Xmx512m"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
