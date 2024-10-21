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

package threshold

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/sirupsen/logrus"
)

const (
	maxMemPrefix  string = "MEM_MAX"
	minFreePrefix string = "FREE_MIN"
	gcPctPrefix   string = "GC_PCT"
)

var groupMaxMem map[string]uint32  // group name -> max value
var groupMinFree map[string]uint32 // group name -> free value
var groupGcPct map[string]uint32   // group name -> percentage of threshold memory, including max and free values

func initMemory() {
	groupMaxMem = make(map[string]uint32)
	groupMinFree = make(map[string]uint32)
	groupGcPct = make(map[string]uint32)

	restoreMemData()
}

func restoreMemData() {
	doRestoreMemDataUint(maxMemPrefix, groupMaxMem)
	doRestoreMemDataUint(minFreePrefix, groupMinFree)
	doRestoreMemDataUint(gcPctPrefix, groupGcPct)
}

func doRestoreMemDataUint(prefix string, dataMap map[string]uint32) {
	buffer := retrieveData(prefix, reflect.Uint32)

	for k, v := range buffer {
		if value, ok := v.(uint32); !ok {
			logrus.Errorf("failed to recover a `%s` config, due to an incorrect value type, %s: %s", prefix, k, v)
			continue
		} else {
			dataMap[k] = value
		}
	}
}

// GroupMaxMemory is the maximum memory size of a worker within a group, measured in megabytes.
// group: group name that worker belongs to
func GroupMaxMemory(group string) uint32 {
	if v, ok := groupMaxMem[group]; ok {
		return v
	}
	return uint32(0)
}

// GroupMinFree is the minimum free memory size of a worker within a group, measured in megabytes.
// group: group name that worker belongs to
func GroupMinFree(group string) uint32 {
	if v, ok := groupMinFree[group]; ok {
		return v
	}
	return uint32(0)
}

// GroupGcPct is the percentage of memory threshold, triggering a GC process on a worker within group, measured in percentage.
func GroupGcPct(group string) uint32 {
	if v, ok := groupGcPct[group]; ok {
		return v
	}
	return uint32(0)
}

func AllGroupMaxMem() map[string]uint32 {
	return groupMaxMem
}

func AllGroupMinFree() map[string]uint32 {
	return groupMinFree
}

func AllGroupGcPct() map[string]uint32 {
	return groupGcPct
}

func SetGroupMaxMem(group string, size uint32) error {
	return doSetGroupUint32(group, size, maxMemPrefix, groupMaxMem)
}

func SetGroupMinFree(group string, size uint32) error {
	return doSetGroupUint32(group, size, minFreePrefix, groupMinFree)
}

func SetGroupGcPct(group string, percentage uint32) error {
	if percentage < 0 || percentage > 100 {
		return errors.New("`GC PCT ` out of range [0.. 100]")
	}
	return doSetGroupUint32(group, percentage, gcPctPrefix, groupGcPct)
}

func doSetGroupUint32(group string, value uint32, prefix string, dataMap map[string]uint32) error {
	if group == "" {
		return fmt.Errorf("the argument 'group' is invalid")
	}

	data := makeData(prefix, group, value)
	process := []assure{func() { dataMap[group] = value }}

	if err := batchSave(data, process); err != nil {
		return err
	}

	return nil
}
