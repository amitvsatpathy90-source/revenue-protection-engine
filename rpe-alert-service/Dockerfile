# Multi-stage: self-contained build, pinned bases (no :latest — infra rule)
FROM maven:3.9.15-eclipse-temurin-26 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package -DskipTests

FROM eclipse-temurin:24.0.2_12-jre
WORKDIR /app
COPY --from=build /build/target/rpe-alert-service-*.jar app.jar
# Non-root by default (defense in depth): the pod securityContext already sets runAsUser:1000 +
# runAsNonRoot, but pinning it here keeps the image non-root even run outside k8s (docker run, ECS).
USER 1000:1000
EXPOSE 8083
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -Dspring.reactor.debug-agent.enabled=false"
ENTRYPOINT ["java", "-jar", "app.jar"]
