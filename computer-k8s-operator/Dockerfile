FROM openjdk:11-jre
LABEL maintainer="HugeGraph Docker Maintainers <hugegraph@googlegroups.com>"
WORKDIR /opt/app
COPY target/hugegraph-computer-operator-*.jar hugegraph-computer-operator.jar
ENTRYPOINT ["java", "-jar", "hugegraph-computer-operator.jar"]
