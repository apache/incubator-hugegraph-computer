FROM hugegraph/hugegraph-computer-framework:latest

LABEL maintainer="HugeGraph Docker Maintainers <hugegraph@googlegroups.com>"

ARG jarFilePath="/opt/jars/hugegraph-builtin-algorithms.jar"
COPY target/computer-algorithm-*.jar ${jarFilePath}
ENV JAR_FILE_PATH=${jarFilePath}
