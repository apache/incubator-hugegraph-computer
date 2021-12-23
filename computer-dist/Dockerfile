FROM mcr.microsoft.com/java/jre:11-zulu-ubuntu
LABEL maintainer="HugeGraph Docker Maintainers <hugegraph@googlegroups.com>"
ENV JAVA_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+UseZGC -XX:+UseCGroupMemoryLimitForHeap -XX:MaxRAMFraction=2 -XshowSettings:vm"
COPY . /etc/local/hugegraph-computer
WORKDIR /etc/local/hugegraph-computer
RUN apt-get update && apt-get -y install gettext-base && apt-get -y install wget
