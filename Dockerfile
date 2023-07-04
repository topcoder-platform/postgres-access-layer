FROM maven:3.8.3-openjdk-17 as builder
COPY src /home/tc-postgres-dal/src
COPY pom.xml /home/tc-postgres-dal
COPY tc-dal-rdb-proto-1.1-SNAPSHOT.jar /home/tc-postgres-dal
RUN mvn install:install-file -Dfile=/home/tc-postgres-dal/tc-dal-rdb-proto-1.1-SNAPSHOT.jar -DgroupId=com.topcoder -DartifactId=tc-dal-rdb-proto -Dversion=1.1-SNAPSHOT -Dpackaging=jar
RUN mvn -f /home/tc-postgres-dal/pom.xml clean
RUN mvn -f /home/tc-postgres-dal/pom.xml package


FROM gcr.io/distroless/java17
COPY --from=builder /home/tc-postgres-dal/target/*.jar /app/postgres-access-layer.jar
ENV DB_URL=""
ENV DB_USERNAME=""
ENV DB_PASSWORD=""
ENTRYPOINT ["java", "-jar", "-Dspring.datasource.url=${DB_URL}", "-Dspring.datasource.username=${DB_USERNAME}", "-Dspring.datasource.password=${DB_PASSWORD}", "/app/postgres-access-layer.jar"]
