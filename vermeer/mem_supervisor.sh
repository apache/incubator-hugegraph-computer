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
