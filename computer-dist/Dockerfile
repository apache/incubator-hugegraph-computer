FROM mcr.microsoft.com/java/jre:11-zulu-ubuntu
LABEL maintainer="HugeGraph Docker Maintainers <hugegraph@googlegroups.com>"
# use ParallelGC is more friendly to olap system
ENV JAVA_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+UseParallelGC -XX:+UseContainerSupport -XX:MaxRAMPercentage=50"
COPY . /etc/local/hugegraph-computer
WORKDIR /etc/local/hugegraph-computer
RUN apt-get update && apt-get -y install gettext-base && apt-get -y install wget
