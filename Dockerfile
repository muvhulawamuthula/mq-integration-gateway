# --- build stage: compile and test, produce the boot jar -------------------------------------
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app

COPY pom.xml .
RUN mvn -B -q dependency:go-offline

COPY src ./src
RUN mvn -B -q clean package

# --- runtime stage: JRE only, non-root ------------------------------------------------------
FROM eclipse-temurin:21-jre
WORKDIR /app

# Run as an unprivileged user.
RUN useradd --system --uid 10001 appuser
USER appuser

COPY --from=build /app/target/mq-integration-gateway-*.jar app.jar

# Target IBM MQ in production.
ENV SPRING_PROFILES_ACTIVE=mq
EXPOSE 8081
HEALTHCHECK --interval=30s --timeout=3s --start-period=40s \
  CMD wget -qO- http://localhost:8081/actuator/health | grep -q UP || exit 1

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
