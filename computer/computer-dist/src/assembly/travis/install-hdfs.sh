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
set -euo pipefail

# Pin ASF's Hadoop image independently of the Java version used to build Computer.
image=apache/hadoop:3.5.0@sha256:46980ea1653eb3453d9cf43c4b7cb69c4729e19612d4902da0372d11e11a7a12
prefix=${HDFS_CONTAINER_PREFIX:-hugegraph-ci-hdfs}
timeout 600 docker pull --platform linux/amd64 "$image"

# The tests run on the Ubuntu host and connect to localhost:9000. Host networking
# also makes the DataNode block address reachable without changing test inputs.
config=(
    --env CORE-SITE.XML_fs.defaultFS=hdfs://localhost:9000
    --env HDFS-SITE.XML_dfs.namenode.rpc-address=localhost:9000
    --env HDFS-SITE.XML_dfs.replication=1
    --env HDFS-SITE.XML_dfs.permissions.enabled=false
    --env HDFS-SITE.XML_dfs.datanode.address=127.0.0.1:9866
)
docker run --detach --platform linux/amd64 --network host --name "$prefix-namenode" \
    "${config[@]}" \
    --env HDFS-SITE.XML_dfs.namenode.name.dir=file:///tmp/hugegraph-hdfs/name \
    --env ENSURE_NAMENODE_DIR=/tmp/hugegraph-hdfs/name \
    "$image" hdfs namenode
docker run --detach --platform linux/amd64 --network host --name "$prefix-datanode" \
    "${config[@]}" "$image" hdfs datanode

deadline=$((SECONDS + ${SERVICE_READY_TIMEOUT:-180}))
while (( SECONDS < deadline )); do
    for service in namenode datanode; do
        if [[ $(docker inspect --format '{{.State.Running}}' "$prefix-$service") != true ]]; then
            docker logs "$prefix-$service" >&2
            echo "HDFS $service exited before readiness" >&2
            exit 1
        fi
    done
    if curl --fail --silent --connect-timeout 2 --max-time 5 \
        'http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState' |
        python3 -c '
import json, sys
sys.exit(not any(b.get("NumLiveDataNodes", 0) > 0 for b in json.load(sys.stdin)["beans"]))
'; then
        # Registration alone does not prove the DataNode accepts block writes.
        timeout 30 docker exec "$prefix-namenode" hdfs dfs -mkdir -p /dataset
        printf 'hugegraph-hdfs-ready\n' |
            timeout 30 docker exec -i "$prefix-namenode" hdfs dfs -put - /dataset/.ci-ready
        [[ $(timeout 30 docker exec "$prefix-namenode" hdfs dfs -cat /dataset/.ci-ready) == hugegraph-hdfs-ready ]]
        timeout 30 docker exec "$prefix-namenode" hdfs dfs -rm /dataset/.ci-ready
        exit 0
    fi
    sleep 2
done
echo 'HDFS readiness timed out' >&2
exit 1
