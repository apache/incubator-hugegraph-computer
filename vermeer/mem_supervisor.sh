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
#! /bin/bash

while true; do
    # body
    # 从/proc/meminfo读取MemAvailable的值
    available_mem=$(cat /proc/meminfo | grep MemAvailable: | awk '{print $2}')

    # 输出可用内存信息
    echo "Available Memory: ${available_mem} kB"

    # 检查可用内存是否低于阈值10GiB
    threshold=10485760
    if [ ${available_mem} -lt ${threshold} ]; then
        echo "Warning: Available memory is below ${threshold} kB!"
        vermeer_pids=$(ps ax | grep vermeer | grep -v "grep" | awk '{print $1}')
        for vermeer_pid in ${vermeer_pids}; do
            echo "Kill Vermeer PID: ${vermeer_pid}"
            kill ${vermeer_pid}
        done
    fi

    sleep 1s

done
