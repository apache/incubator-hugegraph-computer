#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -ev

function print_usage() {
    echo "USAGE: $0 -r {REGISTRY} -u {USER_NAME} -p {PASSWORD} -s {SOURCE_JAR_FILE} -j {JAR_FILE} -i {IMG_URL} -f {FRAMEWORK_IMG_URL}"
}

function build_image() {
    REGISTRY=""
    USER_NAME=""
    PASSWORD=""
    SOURCE_JAR_FILE=""
    JAR_FILE=""
    IMG_URL=""
    FRAMEWORK_IMG_URL=""
    MAINTAINER="HugeGraph Docker Maintainers <dev@hugegraph.apache.org>"

    while getopts "r:u:p:s:j:i:f:" arg; do
      case ${arg} in
          r) REGISTRY="$OPTARG" ;;
          u) USER_NAME="$OPTARG" ;;
          p) PASSWORD="$OPTARG" ;;
          s) SOURCE_JAR_FILE="$OPTARG" ;;
          j) JAR_FILE="$OPTARG" ;;
          i) IMG_URL="$OPTARG" ;;
          f) FRAMEWORK_IMG_URL="$OPTARG" ;;
          ?) print_usage && exit 1 ;;
      esac
    done

    if [ "$SOURCE_JAR_FILE" = "" ]; then
        print_usage
        exit 1
    fi

    if [ "$JAR_FILE" = "" ]; then
        print_usage
        exit 1
    fi

    if [ "$IMG_URL" = "" ]; then
        print_usage
        exit 1
    fi

    if [ "$FRAMEWORK_IMG_URL" = "" ]; then
        print_usage
        exit 1
    fi

    cat >/tmp/upload.txt<<EOF
        # Build image
        SOURCE_JAR_FILE_NAME=${SOURCE_JAR_FILE##*/}
        DOCKER_CONTEXT=${SOURCE_JAR_FILE%/*}
        echo "FROM ${FRAMEWORK_IMG_URL}
              LABEL maintainer='${MAINTAINER}'
              COPY ${SOURCE_JAR_FILE_NAME} ${JAR_FILE}" | \
        docker build -t ${IMG_URL} -f - ${DOCKER_CONTEXT}

        # Login repository
        if [ "$REGISTRY" != "" ]; then
            docker login -u ${USER_NAME} -p ${PASSWORD} ${REGISTRY}
        fi

        # Push image to repository
        docker push ${IMG_URL}

        # Logout repository
        if [ "$REGISTRY" != "" ]; then
            docker logout ${REGISTRY}
        fi
EOF
}
