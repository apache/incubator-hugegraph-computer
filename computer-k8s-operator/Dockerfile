FROM mcr.microsoft.com/java/jre:11-zulu-ubuntu
LABEL maintainer="HugeGraph Docker Maintainers <hugegraph@googlegroups.com>"
WORKDIR /opt/app
COPY target/hugegraph-computer-operator-*.jar hugegraph-computer-operator.jar
ENTRYPOINT ["java", "-jar", "hugegraph-computer-operator.jar"]
