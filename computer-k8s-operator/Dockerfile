FROM openjdk:8-jre-alpine
LABEL maintainer="HugeGraph Docker Maintainers <hugegraph@googlegroups.com>"

ENV JAVA_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap -XX:MaxRAMFraction=2 -XshowSettings:vm"
WORKDIR /opt/app
COPY target/hugegraph-computer-operator-*.jar hugegraph-computer-operator.jar
ENTRYPOINT ["java", "-jar", "hugegraph-computer-operator.jar"]
