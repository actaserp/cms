FROM eclipse-temurin:17-jdk

RUN apt-get update && \
    apt-get install -y maven fonts-noto-cjk fontconfig && \
    rm -rf /var/lib/apt/lists/* && \
    fc-cache -fv

WORKDIR /app

# [핵심] http 차단 정책을 해제하는 settings.xml 생성
RUN mkdir -p /root/.m2 && \
    echo '<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0" \
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" \
      xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 https://maven.apache.org/xsd/settings-1.0.0.xsd"> \
      <mirrors> \
        <mirror> \
          <id>maven-default-http-blocker</id> \
          <mirrorOf>dummy</mirrorOf> \
          <name>Dummy mirror to override default blocking mirror</name> \
          <url>http://0.0.0.0/</url> \
        </mirror> \
      </mirrors> \
    </settings>' > /root/.m2/settings.xml

COPY pom.xml .

# 이제 http 저장소에 접근이 가능해집니다.
RUN mvn dependency:go-offline -B

COPY . .

# 빌드 실패를 방지하기 위해 clean 후 실행
CMD ["mvn", "spring-boot:run", "-Dspring-boot.run.profiles=mes"]