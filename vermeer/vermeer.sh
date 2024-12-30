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
#!/bin/bash
#readonly CUR_SHELL=$(realpath $0)
#readonly CUR_SHELL_DIR=$(dirname $CUR_SHELL)
readonly CUR_SHELL_DIR=$(cd "$(dirname "$0")" && pwd -P)

readonly BIN_DIR=${CUR_SHELL_DIR}
readonly DETECT_PATH="/healthcheck"
readonly APP=vermeer
readonly FUNC_NAME=''
readonly PERIODS=''
readonly LOG_PREFIX=''
readonly LOG_LEVEL="info"
readonly DEBUG_MODE="release"
readonly AUTH="token"
readonly AUTH_TOKEN_FACTOR=1234

#修改为master grpc地址
readonly MASTER_PEER=127.0.0.1:6689

#修改为本机ip
readonly SERVER_IP=0.0.0.0

#若将要启动master，修改此项
readonly MASTER_HTTP_PORT=6688
readonly MASTER_GRPC_PORT=6689

#若将要启动worker，修改此项
readonly WORKER_HTTP_PORT=6788
readonly WORKER_GRPC_PORT=6789

# Start the Vermeer with nohup
start() {
    # Verify if the service is running
    if get_status; then
        echo "The ${APP} ${MODE} is already running"
        exit 0
    else
        # Run the service
        export LD_LIBRARY_PATH="$(pwd)/lib:${LD_LIBRARY_PATH:-}"
        ulimit -n 655350
        nohup $BIN_DIR/${APP} \
            -run_mode=${MODE} \
            -grpc_peer=${SERVER_IP}:$(get_grpc_port) \
            -http_peer=${SERVER_IP}:$(get_http_port) \
            -master_peer=${MASTER_PEER} \
            -log_level=${LOG_LEVEL} \
            -debug_mode=${DEBUG_MODE} \
            -func_name=${FUNC_NAME} \
            -log_prefix=${LOG_PREFIX} \
            -periods=${PERIODS} \
            -auth=${AUTH} \
            -auth_token_factor=${AUTH_TOKEN_FACTOR} \
            >>$BIN_DIR/$APP-${MODE}.log 2>&1 &

        echo "Starting to print log at: $BIN_DIR/$APP-${MODE}.log"
    fi
}

# start the Vermeer without nohup
launch() {
    # Verify if the service is running
    if get_status; then
        echo "The ${APP} ${MODE} is already running"
        exit 0
    else
        # Run the service
        export LD_LIBRARY_PATH="$(pwd)/lib:${LD_LIBRARY_PATH:-}"
        ulimit -n 655350
        $BIN_DIR/${APP} \
            -run_mode=${MODE} \
            -grpc_peer=${SERVER_IP}:$(get_grpc_port) \
            -http_peer=${SERVER_IP}:$(get_http_port) \
            -master_peer=${MASTER_PEER} \
            -log_level=${LOG_LEVEL} \
            -debug_mode=${DEBUG_MODE} \
            -func_name=${FUNC_NAME} \
            -log_prefix=${LOG_PREFIX} \
            -periods=${PERIODS} \
            -auth=${AUTH} \
            -auth_token_factor=${AUTH_TOKEN_FACTOR}
    fi
}

# Stop the Vermeer
stop() {
    local pid=$(get_pid_via_port)

    if [ -n "$pid" ]; then
        echo "Stopping... $APP-$MODE, PID is [ $pid ]"
        echo "For more information, please check the log file at: $BIN_DIR/$APP-${MODE}.log"
        kill $pid
    else
        echo "Failed to get the PID of the $APP instance with TCP port $pid"
    fi
}

# Verify the status of Vermeer
status() {
    if get_status; then
        echo "The $APP $MODE is running."
        exit 0
    else
        echo "The $APP $MODE is stopped."
        exit 1
    fi
}

# Get status of Vermeer to ensure it is alive
get_status() {
    DETECT_URL=${SERVER_IP}:$(get_http_port)${DETECT_PATH}
    HTTP_CODE=$(curl -i -s -w "%{http_code}" -o /dev/null $DETECT_URL)
    if [ "$HTTP_CODE" = 200 ]; then
        return 0
    else
        return 1
    fi
}

get_http_port() {
    case "${MODE}" in
    master)
        echo ${MASTER_HTTP_PORT}
        ;;
    worker)
        echo ${WORKER_HTTP_PORT}
        ;;
    *)
        echo "Unsupported mode: ${MODE}."
        exit 1
        ;;
    esac
}

get_grpc_port() {
    case "${MODE}" in
    master)
        echo ${MASTER_GRPC_PORT}
        ;;
    worker)
        echo ${WORKER_GRPC_PORT}
        ;;
    *)
        echo "Unsupported mode: ${MODE}."
        exit 1
        ;;
    esac
}

get_pid_via_port() {
    local port=$(get_http_port)
    echo $(lsof -i:$port -sTCP:LISTEN | awk 'NR==2{print $2}')
}

# Main logic
CMD=$1
MODE=$2
if [ -z ${MODE} ]; then
    CMD=""
fi
if [ "${MODE}" != "master" -a "${MODE}" != "worker" ]; then
    CMD=""
fi
case "${CMD}" in
start)
    start
    ;;
launch)
    launch
    ;;
stop)
    stop
    ;;
status)
    status
    ;;
restart | reload | rs)
    EXIT=0
    stop
    start
    ;;
*)
    echo $"Usage: $0 {start, stop, status, restart|reload|rs} <master|worker>"
    exit 1
    ;;
esac
exit 0
