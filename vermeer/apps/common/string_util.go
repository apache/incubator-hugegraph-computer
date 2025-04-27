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
	"fmt"
	"runtime"
	"strconv"
	"strings"
)

func ItoaPad(i int, pad int) string {
	result := strconv.Itoa(i)
	if len(result) < pad {
		result = strings.Repeat("0", pad-len(result)) + result
	}
	return result
}

func PrintMemUsage() string {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	alloc := ByteToHuman(m.Alloc)
	totalAlloc := ByteToHuman(m.TotalAlloc)
	sys := ByteToHuman(m.Sys)
	info := fmt.Sprintf("alloc: %v, total: %v, sys: %v, gc: %v", alloc, totalAlloc, sys, m.NumGC)
	return info
}

func ByteToHuman(b uint64) string {
	h := float64(b)
	units := []string{"B", "KB", "MB", "GB", "TB", "PB"}
	unit := 0
	for {
		if h/1024.0 < 1.0 || unit >= len(units)-1 {
			break
		}
		unit++
		h /= 1024
	}
	return fmt.Sprintf("%.3f %s", h, units[unit])
}
