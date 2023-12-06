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

# Note: this script is not used in github-ci now, keep it for other env
set -ev

if [[ $# -ne 1 ]]; then
    echo "Must pass commit id of hugegraph repo"
    exit 1
fi

COMMIT_ID=$1
HUGEGRAPH_GIT_URL="https://github.com/apache/hugegraph.git"

git clone --depth 100 ${HUGEGRAPH_GIT_URL} hugegraph
cd hugegraph
git checkout "${COMMIT_ID}"
mvn package -DskipTests -ntp

mv apache-hugegraph-*.tar.gz ../
cd ../
rm -rf hugegraph
tar zxf apache-hugegraph-*.tar.gz
rm apache-hugegraph-*.tar.gz
cd "$(find apache-hugegraph-* | head -1)"
chmod -R 755 bin/

bin/init-store.sh || exit 1
bin/start-hugegraph.sh || cat logs/hugegraph-server.log

cd ../
