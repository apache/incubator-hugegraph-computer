#!/bin/bash

REGISTRY=""
USER_NAME=""
PASSWORD=""
SOURCE_JAR_FILE=""
JAR_FILE=""
IMG_URL=""

function print_usage() {
    echo "USAGE: $0 -r {REGISTRY} -u {USER_NAME} -p {PASSWORD} -s {SOURCE_JAR_FILE} -j {JAR_FILE} -i {IMG_URL}"
}

while getopts "r:u:p:s:j:i:" arg; do
    case ${arg} in
        r) REGISTRY="$OPTARG" ;;
        u) USER_NAME="$OPTARG" ;;
        p) PASSWORD="$OPTARG" ;;
        s) SOURCE_JAR_FILE="$OPTARG" ;;
        j) JAR_FILE="$OPTARG" ;;
        i) IMG_URL="$OPTARG" ;;
        ?) print_usage && exit 1 ;;
    esac
done

if [ "$USER_NAME" = "" ]; then
    print_usage
    exit 1
fi

if [ "$PASSWORD" = "" ]; then
    print_usage
    exit 1
fi

if [ "$SOURCE_JAR_FILE" = "" ]; then
    print_usage
    exit 1
fi

if [ "$JAR_FILE" = "" ]; then
    print_usage
    exit 1
fi

function abs_path() {
    SOURCE="${BASH_SOURCE[0]}"
    while [ -h "$SOURCE" ]; do
        DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    echo "$( cd -P "$( dirname "$SOURCE" )" && pwd )"
}

BIN=`abs_path`

docker login -u${USER_NAME} -p${PASSWORD} ${REGISTRY}

docker build --build-arg SOURCE_JAR_FILE=${SOURCE_JAR_FILE} JAR_FILE=${JAR_FILE} -t ${IMG_URL} -f ${BIN}/Dockerfile

docker push ${IMG_URL}

docker logout ${REGISTRY}
