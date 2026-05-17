FROM maven:3.9.6-eclipse-temurin-17 AS build

WORKDIR /app

COPY pom.xml ./
COPY shared/pom.xml shared/pom.xml
COPY upload-service/pom.xml upload-service/pom.xml
COPY processing-service/pom.xml processing-service/pom.xml
COPY streaming-service/pom.xml streaming-service/pom.xml

RUN --mount=type=cache,target=/root/.m2 \
  mvn -pl upload-service,processing-service,streaming-service -am dependency:go-offline

COPY . .

RUN --mount=type=cache,target=/root/.m2 \
  mvn -pl upload-service,processing-service,streaming-service -am -DskipTests package

FROM eclipse-temurin:17-jre

RUN apt-get update \
  && apt-get install -y --no-install-recommends ffmpeg python3 python3-pip python3-venv python3-dev gcc build-essential libpq-dev \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=build /app/node-watcher /app/node-watcher
COPY --from=build /app/frontend /app/frontend
COPY --from=build /app/upload-service/target/upload-service-all.jar /app/upload-service.jar
COPY --from=build /app/processing-service/target/processing-service-all.jar /app/processing-service.jar
COPY --from=build /app/streaming-service/target/streaming-service-all.jar /app/streaming-service.jar

RUN python3 -m venv /opt/venv \
  && /opt/venv/bin/pip install --no-cache-dir -r /app/node-watcher/requirements.txt

ENV PATH="/opt/venv/bin:${PATH}"

EXPOSE 8080 8081

CMD ["/bin/sh", "-c", "case \"${SERVICE_MODE:-upload}\" in upload|status) exec java -jar /app/upload-service.jar ;; processing) exec java -jar /app/processing-service.jar ;; streaming) exec java -jar /app/streaming-service.jar ;; *) echo \"Unknown SERVICE_MODE=${SERVICE_MODE:-upload}\" >&2; exit 1 ;; esac"]
