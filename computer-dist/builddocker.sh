#!/bin/env bash

BASE_DIR=$(cd "$(dirname "$0")" && pwd -P)
PROJECT_PATH="$(cd "${BASE_DIR}/.." && pwd -P)"
PROJECT_POM_PATH="${PROJECT_PATH}/pom.xml"
mvn -f "${PROJECT_POM_PATH}" clean package -DskipTests

contextPath=$(mvn -f "${PROJECT_POM_PATH}" -q -N \
org.codehaus.mojo:exec-maven-plugin:1.3.1:exec \
-Dexec.executable='echo' -Dexec.args='${final.name}')
contextPath="${PROJECT_PATH}/${contextPath}"

projectVersion=$(mvn -f "${PROJECT_POM_PATH}" -q -N \
org.codehaus.mojo:exec-maven-plugin:1.3.1:exec \
-Dexec.executable='echo' -Dexec.args='${project.version}')

docker build -t "hugegraph-computer-framework:${projectVersion}" "${contextPath}" \
-f "${BASE_DIR}"/Dockerfile
