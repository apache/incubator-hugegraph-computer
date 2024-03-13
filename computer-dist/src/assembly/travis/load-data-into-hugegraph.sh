#!/usr/bin/env bash
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

TRAVIS_DIR=$(dirname "$0")
DATASET_DIR=${TRAVIS_DIR}/../dataset

docker network create ci
# Note: we need wait for server start finished, so start it first
docker run -itd --name=graph --network ci -p 8080:8080 hugegraph/hugegraph:latest && sleep 6

wget http://files.grouplens.org/datasets/movielens/ml-latest-small.zip
unzip -d ${DATASET_DIR} ml-latest-small.zip

cd ${DATASET_DIR}/.. && pwd && ls -lh *

docker run -id --name=loader --network ci hugegraph/loader:latest
docker cp dataset loader:/dataset || exit 1

docker exec -i loader ls -lh /dataset
docker exec -i loader bin/hugegraph-loader.sh -g hugegraph -p 8080 -h graph \
    -f /dataset/struct.json -s /dataset/schema.groovy || exit 1

# load dataset to hdfs
sort -t , -k1n -u dataset/ml-latest-small/ratings.csv | cut -d "," -f 1 >dataset/ml-latest-small/user_id.csv || exit 1
/opt/hadoop/bin/hadoop fs -mkdir -p /dataset/ml-latest-small || exit 1
/opt/hadoop/bin/hadoop fs -put dataset/ml-latest-small/* /dataset/ml-latest-small || exit 1
/opt/hadoop/bin/hadoop fs -ls /dataset/ml-latest-small

echo "Load finished, continue to next step"

############# Note: this part is not used in github-ci now, backup it for other env ##############
#HUGEGRAPH_LOADER_GIT_URL="https://github.com/apache/hugegraph-toolchain.git"
#git clone --depth 10 ${HUGEGRAPH_LOADER_GIT_URL} hugegraph-toolchain
#
#cd hugegraph-toolchain
#mvn install -P stage -pl hugegraph-client,hugegraph-loader -am -DskipTests -ntp
#
#cd hugegraph-loader
#tar -zxf target/apache-hugegraph-loader-*.tar.gz || exit 1
#cd ../../

#wget http://files.grouplens.org/datasets/movielens/ml-latest-small.zip
#unzip -d ${DATASET_DIR} ml-latest-small.zip

#hugegraph-toolchain/hugegraph-loader/apache-hugegraph-loader-*/bin/hugegraph-loader.sh \
#    -g hugegraph -f ${DATASET_DIR}/struct.json -s ${DATASET_DIR}/schema.groovy || exit 1
