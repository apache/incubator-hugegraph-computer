#!/usr/bin/env bash

set -ev

if [[ $# -ne 1 ]]; then
    echo "Must pass commit id of hugegraph repo"
    exit 1
fi

COMMIT_ID=$1
HUGEGRAPH_GIT_URL="https://github.com/apache/hugegraph.git"

git clone --depth 300 ${HUGEGRAPH_GIT_URL}
cd hugegraph
git checkout ${COMMIT_ID}
mvn package -DskipTests
mv hugegraph-*.tar.gz ../
cd ../
rm -rf hugegraph
tar -zxf hugegraph-*.tar.gz

cd "$(find hugegraph-* | head -1)"
bin/init-store.sh || exit 1
bin/start-hugegraph.sh || exit 1
cd ../
