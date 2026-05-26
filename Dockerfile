FROM eclipse-temurin:17-jre-jammy

RUN apt-get update && \
    apt-get install -y fonts-noto-cjk fontconfig && \
    rm -rf /var/lib/apt/lists/* && \
    fc-cache -fv

WORKDIR /app

RUN mkdir -p /app/data/cms

COPY mes-0.0.1-SNAPSHOT.jar app.jar

ENTRYPOINT ["java", \
  "-Dspring.profiles.active=cms,ncp", \
  "-jar", \
  "app.jar"]