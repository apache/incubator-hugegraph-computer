#!/usr/bin/env bash

set -ev

TRAVIS_DIR=$(dirname "$0")
DATASET_DIR=${TRAVIS_DIR}/../dataset
LIB="assembly/static/lib"

HUGEGRAPH_LOADER_GIT_URL="https://github.com/starhugegraph/hugegraph-loader.git"

git clone -b "${BRANCH}" --depth 10 ${HUGEGRAPH_LOADER_GIT_URL}

cd hugegraph-loader
mvn install:install-file -Dfile=$LIB/ojdbc8-12.2.0.1.jar -DgroupId=com.oracle -DartifactId=ojdbc8 -Dversion=12.2.0.1 -Dpackaging=jar
mvn install:install-file -Dfile=$LIB/hugegraph-client-1.9.7.jar -DgroupId=com.baidu.hugegraph -DartifactId=hugegraph-client -Dversion=1.9.7 -Dpackaging=jar -DpomFile=$LIB/client-pom.xml
mvn install:install-file -Dfile=$LIB/hugegraph-common-1.8.9.jar -DgroupId=com.baidu.hugegraph -DartifactId=hugegraph-common -Dversion=1.8.9 -Dpackaging=jar -DpomFile=$LIB/common-pom.xml
mvn clean package -DskipTests || exit 1
tar -zxf hugegraph-loader-*.tar.gz || exit 1
cd ../

wget http://files.grouplens.org/datasets/movielens/ml-latest-small.zip
unzip -d "${DATASET_DIR}" ml-latest-small.zip

hugegraph-loader/hugegraph-loader-*/bin/hugegraph-loader.sh -g hugegraph -f "${DATASET_DIR}"/struct.json -s "${DATASET_DIR}"/schema.groovy || exit 1

# load dataset to hdfs
sort -t , -k1n -u "${DATASET_DIR}"/ml-latest-small/ratings.csv | cut -d "," -f 1 > "${DATASET_DIR}"/ml-latest-small/user_id.csv || exit 1
hadoop fs -mkdir -p /dataset/ml-latest-small || exit 1
hadoop fs -put "${DATASET_DIR}"/ml-latest-small/* /dataset/ml-latest-small || exit 1
hadoop fs -ls /dataset/ml-latest-small

echo "Load finished, continue to next step"
