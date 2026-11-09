FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S app && adduser -S -G app app
WORKDIR /app

COPY target/data-ingestor.jar /app/data-ingestor.jar
USER app

EXPOSE 9095

ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "/app/data-ingestor.jar"]
