# builder to compile serverledge's executor
FROM eclipse-temurin:21-jdk-alpine AS builder

WORKDIR /build

# gson to handle requests/responses json
RUN wget -O gson.jar https://repo1.maven.org/maven2/com/google/code/gson/gson/2.13.2/gson-2.13.2.jar

COPY Executor.java .

RUN javac -cp .:gson.jar Executor.java


FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

RUN mkdir -p /app/

COPY --from=builder /build/*.class /app/
COPY --from=builder /build/gson.jar /app/

CMD ["java", "-cp", ".:gson.jar", "Executor"]