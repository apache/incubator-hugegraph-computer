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

TRAVIS_DIR=$(dirname $0)
echo "Starting etcd..."
# TODO: replace with docker way
wget -O ${TRAVIS_DIR}/etcd.tar.gz https://github.com/etcd-io/etcd/releases/download/v3.5.0/etcd-v3.5.0-linux-amd64.tar.gz
mkdir ${TRAVIS_DIR}/etcd
tar -zxvf ${TRAVIS_DIR}/etcd.tar.gz -C ${TRAVIS_DIR}/etcd --strip-components 1
chmod a+x ${TRAVIS_DIR}/etcd
${TRAVIS_DIR}/etcd/etcd --name etcd-test \
    --initial-advertise-peer-urls http://localhost:2580 \
    --listen-peer-urls http://localhost:2580 \
    --advertise-client-urls ${BSP_ETCD_URL} \
    --listen-client-urls ${BSP_ETCD_URL} &
