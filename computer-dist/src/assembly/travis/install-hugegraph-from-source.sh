#!/bin/bash

set -ev

if [[ $# -ne 1 ]]; then
    echo "Must pass base branch name of pull request"
    exit 1
fi

CLIENT_BRANCH=$1
HUGEGRAPH_BRANCH=${CLIENT_BRANCH}
HUGEGRAPH_GIT_URL="https://github.com/hugegraph/hugegraph.git"

git clone --depth 100 ${HUGEGRAPH_GIT_URL}
cd hugegraph
git checkout ${COMMIT_ID}
mvn package -DskipTests
mv hugegraph-*.tar.gz ../
cd ../
rm -rf hugegraph
tar -zxf hugegraph-*.tar.gz

cd hugegraph-*
# start HugeGraphServer with http protocol
bin/init-store.sh || exit 1
bin/start-hugegraph.sh || exit 1
cd ../
