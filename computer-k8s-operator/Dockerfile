FROM mcr.microsoft.com/java/jre:11-zulu-ubuntu
LABEL maintainer="HugeGraph Docker Maintainers <hugegraph@googlegroups.com>"
ENV JAVA_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+UseZGC -XX:+UseCGroupMemoryLimitForHeap -XX:MaxRAMFraction=2 -XshowSettings:vm"
WORKDIR /opt/app
COPY target/hugegraph-computer-operator-*.jar hugegraph-computer-operator.jar
ENTRYPOINT ["java", "-jar", "hugegraph-computer-operator.jar"]
