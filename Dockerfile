FROM maven:3.9.6-eclipse-temurin-17

RUN apt-get update \
  && apt-get install -y --no-install-recommends ffmpeg python3 python3-pip \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN python3 -m pip install --no-cache-dir -r node-watcher/requirements.txt

RUN mvn -pl upload-service,processing-service -am -DskipTests install

EXPOSE 8080 8081

CMD ["/bin/sh", "-c", "mvn -pl upload-service -DskipTests exec:java -Dexec.mainClass=com.distributed26.videostreaming.upload.upload.UploadServiceApplication"]
