#!/bin/env bash
set -e
BIN_DIR=$(cd "$(dirname "$0")" && pwd -P)
BASE_DIR=$(cd "${BIN_DIR}/.." && pwd -P)
LIB_DIR=${BASE_DIR}/lib
CONF_DIR="${BASE_DIR}/conf"

COMPUTER_CONF_PATH="${COMPUTER_CONF_PATH}"
LOG4J_XML_PATH="${LOG4J_XML_PATH}"
JAR_FILE_PATH="${JAR_FILE_PATH}"
DRIVE=
K8S_DRIVE="k8s"
YARN_DRIVE="yarn"
LOCAL_DRIVE="local"
ROLE=
ROLE_MASTER="master"
ROLE_WORKER="worker"

usage() {
    echo "start-computer.sh <-c|--conf conf_file_path> <-a|--algorithm"
    echo "algorithm_jar_path> [-l|--log4 log4_conf_path] <-d|--drive drive_type"
    echo "(local|k8s|yarn)>"
}

if [ $# -lt 4 ];
    then usage
fi

check_empty() {
    if [ "$2" = "" ]; then
        echo $1
        exit 1
    fi
}

check_file_readable() {
    if [ ! -r "$2" ];then
        echo $1
        exit 1
    fi
}

check_file_executable() {
    if [ ! -x "$1" ];then
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
                check_empty "conf file not be empty" $2
                check_file_readable "file $2 not be read permission" $2
                COMPUTER_CONF_PATH=$2
                shift 2 ;;
            -l|--log4)
                check_empty "log conf file not be empty" $2
                check_file_readable "file $2 not be read permission" $2
                LOG4J_XML_PATH=$2
                shift 2 ;;
            -a|--algorithm)
                check_empty "algorithm jar file not be empty" $2
                check_file_readable "file $2 not be read permission" $2
                JAR_FILE_PATH=$2
                shift 2 ;;
            -d|--drive)
                check_empty "drive not be empty" $1
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
                        echo "unknown drive %2, muse be k8s|yarn|local"
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
                        echo "unknown role %2, muse be master|worker"
                        exit 1
                esac
                shift 2;;
            *) echo "unknown options -$1-" ; usage; exit 1 ;;
        esac
    done
}

parse_opts $*

echo "COMPUTER_CONF_PATH=${COMPUTER_CONF_PATH}"
echo "LOG4J_XML_PATH=${LOG4J_XML_PATH}"
echo "JAR_FILE_PATH=${JAR_FILE_PATH}"
echo "DRIVE=${DRIVE}"

if [ "${JAR_FILE_PATH}" = "" ]; then
    echo "graph algorithm jar file missed";
    usage;
    exit 1;
fi

if [ "${COMPUTER_CONF_PATH}" = "" ]; then
    echo "conf file missed";
    usage;
    exit 1;
fi

if [ "${DRIVE}" = "" ]; then
    echo "drive is missed";
    usage;
    exit 1;
fi

if [ "${ROLE}" = "" ]; then
    echo "role is missed";
    usage;
    exit 1;
fi

CP=$(find "${LIB_DIR}" -name "*.jar" | tr "\n" ":")

CP="$JAR_FILE_PATH":${CP}

# Find Java
if [ "$JAVA_HOME" = "" ]; then
    JAVA="java -server"
else
    JAVA=""$JAVA_HOME/bin/java" -server"
fi

if [ ! -a "${CONF_DIR}" ];then
    mkdir -p "${CONF_DIR}"
fi

COPY_CONF_DIR="${CONF_DIR}/copy"
if [ ! -a "${COPY_CONF_DIR}" ]; then
    mkdir -p "${COPY_CONF_DIR}"
    chmod 777 "${COPY_CONF_DIR}"
fi

NEW_COMPUTER_CONF_PATH="${COPY_CONF_DIR}/$(basename "${COMPUTER_CONF_PATH}")"
envsubst <"${COMPUTER_CONF_PATH}" >"${NEW_COMPUTER_CONF_PATH}"
chmod 777 "${NEW_COMPUTER_CONF_PATH}"

JVM_OPTIONS=${JVM_OPTIONS}
if [ "${LOG4J_XML_PATH}" != "" ];then
    JVM_OPTIONS="${JVM_OPTIONS} -Dlog4j.configurationFile="${LOG4J_XML_PATH}""
fi

${JAVA} -Dname="hugegraph-computer" ${JVM_OPTIONS} -cp "${CP}" \
com.baidu.hugegraph.computer.dist.HugeGraphComputer \
"${NEW_COMPUTER_CONF_PATH}" ${ROLE} ${DRIVE}
