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

package common

import (
	"os"
	"path/filepath"

	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/process"
	"github.com/sirupsen/logrus"
)

func AppRootPath() string {
	path, err := GetCurrentPath()
	if err != nil {
		logrus.Errorf("failed to get `AppRootPath`, err: %v", err)
		return ""
	}
	return path
}

func GetCurrentPath() (string, error) {
	ex, err := os.Executable()
	if err != nil {
		return "", err
	}
	currentPath := filepath.Dir(ex)
	return currentPath, nil
}

func IsFileOrDirExist(dir string) bool {
	_, err := os.Stat(dir)
	return err == nil
}

func MachineMemUsedPercent() (float64, error) {
	v, err := mem.VirtualMemory()
	return v.UsedPercent, err
}

func ProcessMemUsedPercent() (float64, error) {
	pid := os.Getpid()
	proc, err := process.NewProcess(int32(pid))

	if err != nil {
		return 0, err
	}
	p, err := proc.MemoryPercent()
	return float64(p), err
}
