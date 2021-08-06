#!/usr/bin/env bash

BASE_DIR=$(cd "$(dirname "$0")" && pwd -P)
PROJECT_PATH="$(cd "${BASE_DIR}/.." && pwd -P)"
PROJECT_POM_PATH="${PROJECT_PATH}/pom.xml"
mvn -f "${PROJECT_POM_PATH}" clean package -DskipTests

CONTEXT_PATH=$(mvn -f "${PROJECT_POM_PATH}" -q -N \
    org.codehaus.mojo:exec-maven-plugin:1.3.1:exec \
    -Dexec.executable='echo' -Dexec.args='${final.name}')
    CONTEXT_PATH="${PROJECT_PATH}/${CONTEXT_PATH}"

PROJECT_VERSION=$(mvn -f "${PROJECT_POM_PATH}" -q -N \
    org.codehaus.mojo:exec-maven-plugin:1.3.1:exec \
    -Dexec.executable='echo' -Dexec.args='${project.version}')

docker build -t "hugegraph/hugegraph-computer-framework:${PROJECT_VERSION}" \
    "${CONTEXT_PATH}" -f "${BASE_DIR}"/Dockerfile
