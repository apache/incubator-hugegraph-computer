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

BIN_DIR=$(cd "$(dirname "$0")" && pwd -P)
BASE_DIR=$(cd "${BIN_DIR}/.." && pwd -P)
LIB_DIR=${BASE_DIR}/lib
CONF_DIR="${BASE_DIR}/conf"
DEFAULT_ALGORITHM_DIR="${BASE_DIR}/algorithm"

COMPUTER_CONF_PATH="${COMPUTER_CONF_PATH}"
LOG4J_CONF_PATH="${LOG4J_CONF_PATH}"
JAR_FILE_PATH="${JAR_FILE_PATH}"
DRIVE=
K8S_DRIVE="k8s"
YARN_DRIVE="yarn"
LOCAL_DRIVE="local"
ROLE=
ROLE_MASTER="master"
ROLE_WORKER="worker"

usage() {
    echo "Usage:"
    echo "    start-computer.sh"
    echo "        [-c|--conf conf_file_path>"
    echo "        [-a|--algorithm algorithm_jar_path>"
    echo "        [-l|--log4 log4_conf_path]"
    echo "        <-d|--drive drive_type(local|k8s|yarn)>"
    echo "        <-r|--role role(master|worker)>"
}

if [ $# -lt 4 ]; then
    usage
fi

check_empty() {
    if [ "$2" = "" ]; then
        echo $1
        exit 1
    fi
}

check_file_readable() {
    if [ ! -r "$2" ]; then
        echo $1
        exit 1
    fi
}

check_file_executable() {
    if [ ! -x "$1" ]; then
        echo $2
        exit 1
    fi
}

parse_opts() {
    while true ; do
        if [ -z "$1" ]; then
            break
        fi

        case "$1" in
            -c|--conf)
                check_empty "conf file can't be empty" $2
                check_file_readable "file $2 doesn't have read permission" $2
                COMPUTER_CONF_PATH=$2
                shift 2 ;;
            -l|--log4)
                check_empty "log conf file can't be empty" $2
                check_file_readable "file $2 doesn't have read permission" $2
                LOG4J_CONF_PATH=$2
                shift 2 ;;
            -a|--algorithm)
                check_empty "algorithm jar file can't be empty" $2
                check_file_readable "file $2 doesn't have read permission" $2
                JAR_FILE_PATH=$2
                shift 2 ;;
            -d|--drive)
                check_empty "driver can't be empty" $1
                case "$2" in
                    ${K8S_DRIVE})
                        DRIVE=${K8S_DRIVE}
                        ;;
                    ${YARN_DRIVE})
                        DRIVE=${YARN_DRIVE}
                        ;;
                    ${LOCAL_DRIVE})
                        DRIVE=${LOCAL_DRIVE}
                        ;;
                    *)
                        echo "unknown drive %2, must be k8s|yarn|local"
                        exit 1
                esac
                shift 2;;
            -r|--role)
                case "$2" in
                    ${ROLE_MASTER})
                        ROLE=${ROLE_MASTER}
                        ;;
                    ${ROLE_WORKER})
                        ROLE=${ROLE_WORKER}
                        ;;
                    *)
                        echo "unknown role %2, must be master|worker"
                        exit 1
                esac
                shift 2;;
            *) echo "unknown options -$1-" ; usage; exit 1 ;;
        esac
    done
}

parse_opts $*

if [ "${COMPUTER_CONF_PATH}" = "" ]; then
    if [ "$DRIVE" = "$K8S_DRIVE" ]; then
      echo "conf file is missing";
      usage;
      exit 1;
    fi
    COMPUTER_CONF_PATH=${CONF_DIR}/computer.properties
fi

if [ "${DRIVE}" = "" ]; then
    echo "drive is missing";
    usage;
    exit 1;
fi

if [ "${ROLE}" = "" ]; then
    echo "role is missing";
    usage;
    exit 1;
fi

if [ "${LOG4J_CONF_PATH}" = "" ];then
    LOG4J_CONF_PATH=${CONF_DIR}/log4j2.xml
fi

echo "************************************"
echo "COMPUTER_CONF_PATH=${COMPUTER_CONF_PATH}"
echo "LOG4J_CONF_PATH=${LOG4J_CONF_PATH}"
echo "ALGORITHM_JAR_FILE_PATH=${JAR_FILE_PATH}"
echo "DRIVE=${DRIVE}"
echo "ROLE=${ROLE}"
echo "************************************"

CP=$(find "${LIB_DIR}" -name "*.jar" | tr "\n" ":")

CP=${CP}:"${DEFAULT_ALGORITHM_DIR}/*"

if [ "${JAR_FILE_PATH}" != "" ]; then
    CP=${CP}:${JAR_FILE_PATH}
fi

# Download remote job JAR file.
if [[ "${REMOTE_JAR_URI}" == http://* || "${REMOTE_JAR_URI}" == https://* ]]; then
    mkdir -p "${BASE_DIR}/job"
    echo "Downloading job JAR ${REMOTE_JAR_URI} to ${BASE_DIR}/job/"
    wget -nv -P "${BASE_DIR}/job/" "${REMOTE_JAR_URI}"
    JOB_JAR=$(find "${BASE_DIR}/job" -name "*.jar" | tr "\n" ":")
    if [[ "$JOB_JAR" != "" ]]; then
        CP="${JOB_JAR}"$CP
    fi
elif [[ "${REMOTE_JAR_URI}" != "" ]]; then
    echo "Unsupported protocol for ${REMOTE_JAR_URI}"
    exit 1
fi

# Find Java
if [ "$JAVA_HOME" = "" ]; then
    JAVA="java -server"
else
    JAVA="$JAVA_HOME/bin/java -server"
fi

# Set up count of cpu if it unspecified from k8s drive,
# avoid `Runtime.getRuntime().availableProcessors()` always return 1
if [[ "${DRIVE}" = "${K8S_DRIVE}" && -z "${CPU_LIMIT}" ]]; then
    PROCESSOR_COUNT="$(cat /proc/cpuinfo | grep "processor" | wc -l)"
    let MAX_PROCESSOR_COUNT=8
    if [[ ${PROCESSOR_COUNT} -gt ${MAX_PROCESSOR_COUNT} ]]; then
      PROCESSOR_COUNT="$MAX_PROCESSOR_COUNT"
    fi
    JAVA_OPTS="${JAVA_OPTS} -XX:ActiveProcessorCount=${PROCESSOR_COUNT}"
fi

if [ ! -a "${CONF_DIR}" ];then
    mkdir -p "${CONF_DIR}"
fi

if [ "$DRIVE" = "$K8S_DRIVE" ]; then
    COPY_CONF_DIR="${CONF_DIR}/copy"
    if [ ! -a "${COPY_CONF_DIR}" ]; then
        mkdir -p "${COPY_CONF_DIR}"
        chmod 777 "${COPY_CONF_DIR}"
    fi

    NEW_COMPUTER_CONF_PATH="${COPY_CONF_DIR}/$(basename "${COMPUTER_CONF_PATH}")"
    envsubst '${POD_IP},${HOSTNAME},${POD_NAME},${POD_NAMESPACE}' < "${COMPUTER_CONF_PATH}" > "${NEW_COMPUTER_CONF_PATH}"
    chmod 777 "${NEW_COMPUTER_CONF_PATH}"
    COMPUTER_CONF_PATH=${NEW_COMPUTER_CONF_PATH}
fi

LOG4j_CONF=-Dlog4j.configurationFile="${LOG4J_CONF_PATH}"

if [ "${ROLE}" = "${ROLE_MASTER}" ]; then
    LOG_NAME=-Dlog.name=hugegraph-computer-master
fi

if [ "${ROLE}" = "${ROLE_WORKER}" ]; then
    LOG_NAME=-Dlog.name=hugegraph-computer-worker
fi

MAIN_CLASS=org.apache.hugegraph.computer.dist.HugeGraphComputer

exec ${JAVA} -Dname="hugegraph-computer" "${LOG4j_CONF}" ${LOG_NAME} ${JAVA_OPTS} ${JVM_OPTIONS} \
        -cp "${CP}" ${MAIN_CLASS} "${COMPUTER_CONF_PATH}" ${ROLE} ${DRIVE}
