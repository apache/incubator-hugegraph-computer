FROM openjdk:8-jre-alpine

LABEL maintainer="HugeGraph Docker Maintainers <hugegraph@googlegroups.com>"

ENV JAVA_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap -XX:MaxRAMFraction=2 -XshowSettings:vm"

WORKDIR /opt/app
COPY target/hugegraph-computer-operator.jar operator.jar
EXPOSE 8081

CMD ["java", "-jar", "operator.jar"]
