FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

COPY pom.xml .
RUN mvn -B -q dependency:resolve

COPY src ./src
RUN mvn -B -q package -DskipTests \
 && mvn -B -q dependency:copy-dependencies -DoutputDirectory=target/lib -DincludeScope=runtime

FROM eclipse-temurin:21-jre
WORKDIR /app

RUN useradd --system --create-home appuser
COPY --from=build /build/target/classes ./classes
COPY --from=build /build/target/lib ./lib
USER appuser

EXPOSE 50051
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-cp", "classes:lib/*", "iuh.fit.grpc.GrpcServerApp"]
