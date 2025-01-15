/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with this
work for additional information regarding copyright ownership. The ASF
licenses this file to You under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations
under the License.
*/

package worker

import (
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
	"vermeer/apps/common"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/process"
	"github.com/sirupsen/logrus"
)

var RuntimeSpv RuntimeSupervisor

func init() {
	RuntimeSpv = RuntimeSupervisor{}
}

type LimitMemory struct {
	MaxMemUsed   uint64
	MinRemainMem uint64
}

type HostInfo struct {
	TotalMemory     uint64
	AvailableMemory uint64
	CPUs            int
}

type RuntimeSupervisor struct {
	sync.Mutex
	MemoryChan chan LimitMemory
}

func (rs *RuntimeSupervisor) HostInfo() (HostInfo, error) {
	rs.Lock()
	defer rs.Unlock()
	memory, err := mem.VirtualMemory()
	if err != nil {
		logrus.Errorf("get vitual memory error:%v", err)
		return HostInfo{}, err
	}
	cpus, err := cpu.Counts(true)
	if err != nil {
		logrus.Errorf("get logic cpu count error:%v", err)
		return HostInfo{}, err
	}
	logrus.Infof("host memory total:%v, available:%v, logic cpu count:%v", memory.Total, memory.Available, cpus)
	return HostInfo{
		TotalMemory:     memory.Total,
		AvailableMemory: memory.Available,
		CPUs:            cpus,
	}, nil
}

func (rs *RuntimeSupervisor) SetMaxCPU(maxCPU int) {
	rs.Lock()
	defer rs.Unlock()
	if maxCPU < 1 {
		logrus.Infof("set max cpu is less than 1, set to 1. max cpu:%v", maxCPU)
		maxCPU = 1
	}
	if maxCPU > runtime.NumCPU() {
		logrus.Infof("set max cpu is more than num cpu, set to num cpu:%v. max cpu:%v", runtime.NumCPU(), maxCPU)
		maxCPU = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(maxCPU)
}

func (rs *RuntimeSupervisor) SetMemoryLimit(maxMemoryUsed uint64, minRemainMemory uint64, softMemoryLimitRatio float32) {
	rs.Lock()
	defer rs.Unlock()
	memory, err := mem.VirtualMemory()
	if err != nil {
		logrus.Errorf("get vitual memory error:%v", err)
		return
	}
	if maxMemoryUsed > memory.Total || maxMemoryUsed <= 0 {
		maxMemoryUsed = memory.Total
	}
	if minRemainMemory > memory.Available || minRemainMemory <= 0 {
		minRemainMemory = 0
	}
	if softMemoryLimitRatio >= 1 || softMemoryLimitRatio < 0 {
		softMemoryLimitRatio = 0
	}
	softMemoryLimit := int64(float64(1-softMemoryLimitRatio) * float64(maxMemoryUsed))
	debug.SetMemoryLimit(softMemoryLimit)

	if rs.MemoryChan == nil {
		rs.MemoryChan = make(chan LimitMemory, 16)
		go rs.superviseMemory()
	}
	rs.MemoryChan <- LimitMemory{
		MaxMemUsed:   maxMemoryUsed,
		MinRemainMem: minRemainMemory,
	}
}

// superviseMemory 监控内存使用情况
func (rs *RuntimeSupervisor) superviseMemory() {
	vermeerProcess, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		logrus.Errorf("create supervise process error:%v", err)
		return
	}
	var memoryLimit LimitMemory
	for {
		select {
		case memoryLimit = <-rs.MemoryChan:
			logrus.Infof("set memory limit:%v", memoryLimit)
		default:
			memory, err := mem.VirtualMemory()
			if err != nil {
				logrus.Errorf("supervise memory error:%v", err)
				continue
			}
			if memory.Available < memoryLimit.MinRemainMem {
				//可用内存小于最小可用内存，立即执行一次GC，释放内存后再次判断，如果仍然小于，直接退出程序
				runtime.GC()
				debug.FreeOSMemory()
				memory, err = mem.VirtualMemory()
				if err != nil {
					logrus.Errorf("supervise memory error:%v", err)
					continue
				}
				if memory.Available < memoryLimit.MinRemainMem {
					logrus.Fatalf("available memory not enough. minimum remaining:%v, available:%v",
						common.ByteToHuman(memoryLimit.MinRemainMem), common.ByteToHuman(memory.Available))
				}
			}
			info, err := vermeerProcess.MemoryInfo()
			if err != nil {
				logrus.Errorf("get vermeer process memory error:%v", err)
				continue
			}
			if info.RSS > memoryLimit.MaxMemUsed {
				logrus.Fatalf("used memory exceeds limit. max limit:%v, vermeer process res:%v",
					common.ByteToHuman(memoryLimit.MaxMemUsed), common.ByteToHuman(info.RSS))
			}

			logrus.Debugf("memory avialable:%v, rss:%v", common.ByteToHuman(memory.Available), common.ByteToHuman(info.RSS))
			time.Sleep(1 * time.Minute)
		}
	}
}
