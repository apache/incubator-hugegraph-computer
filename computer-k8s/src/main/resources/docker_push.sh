#!/bin/bash

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
    MAINTAINER="HugeGraph Docker Maintainers <hugegraph@googlegroups.com>"

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

    # Build image
    SOURCE_JAR_FILE_NAME=${SOURCE_JAR_FILE##*/}
    DOCKER_CONTEXT=${SOURCE_JAR_FILE%/*}
    echo "FROM ${FRAMEWORK_IMG_URL}
          LABEL maintainer='${MAINTAINER}'
          COPY ${source_jar_file_name} ${JAR_FILE}" | \
    docker build -t ${IMG_URL} -f - ${docker_context}

    # Login repository
    if [ "$USER_NAME" != "" ]; then
        docker login -u ${USER_NAME} -p ${PASSWORD} ${REGISTRY}
    fi

    # Push image to repository
    docker push ${IMG_URL}

    # Logout repository
    if [ "$USER_NAME" != "" ]; then
        docker logout ${REGISTRY}
    fi
}
