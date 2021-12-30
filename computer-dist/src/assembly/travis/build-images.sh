#!/usr/bin/env bash

set -ev

BASE_DIR=$(cd "$(dirname "$0")" && pwd -P)
PROJECT_PATH="$(cd "${BASE_DIR}/../../../.." && pwd -P)"
PROJECT_POM_PATH="${PROJECT_PATH}/pom.xml"
JAR_FILE_PATH=/opt/jars/hugegraph-builtin-algorithms.jar

mvn -f "${PROJECT_POM_PATH}" clean package -DskipTests $4

CONTEXT_PATH=$(mvn -f "${PROJECT_POM_PATH}" -q -N \
    org.codehaus.mojo:exec-maven-plugin:1.3.1:exec \
    -Dexec.executable='echo' -Dexec.args='${final.name}')
    CONTEXT_PATH="${PROJECT_PATH}/${CONTEXT_PATH}"

PROJECT_VERSION=$(mvn -f "${PROJECT_POM_PATH}" -q -N \
    org.codehaus.mojo:exec-maven-plugin:1.3.1:exec \
    -Dexec.executable='echo' -Dexec.args='${project.version}')

docker build -t $1 $CONTEXT_PATH -f $PROJECT_PATH/computer-dist/Dockerfile

echo "FROM $1
LABEL maintainer='HugeGraph Docker Maintainers <hugegraph@googlegroups.com>'
COPY target/computer-algorithm-*.jar $JAR_FILE_PATH
ENV JAR_FILE_PATH=$JAR_FILE_PATH" | \
docker build -t $2 -f - $PROJECT_PATH/computer-algorithm

docker build -t $3 -f $PROJECT_PATH/computer-k8s-operator/Dockerfile $PROJECT_PATH/computer-k8s-operator
