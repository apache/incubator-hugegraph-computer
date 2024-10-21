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
	"sync/atomic"
	"time"
)

type SpinLocker uint32

func (sl *SpinLocker) Lock() {
	for i := 0; ; i++ {
		if atomic.CompareAndSwapUint32((*uint32)(sl), 0, 1) {
			break
		}
		if i >= 1000 {
			i = 0
			//logrus.Warnf("spin lock busy")
			time.Sleep(1 * time.Millisecond)
		}
	}
}

func (sl *SpinLocker) UnLock() {
	atomic.StoreUint32((*uint32)(sl), 0)
}

type SpinWaiter int32

func (sw *SpinWaiter) Reset() {
	atomic.StoreInt32((*int32)(sw), 0)
}

func (sw *SpinWaiter) Add(n int32) {
	atomic.AddInt32((*int32)(sw), n)
}

func (sw *SpinWaiter) Done() {
	sw.Add(-1)
}

func (sw *SpinWaiter) Wait() {
	for i := 0; ; i++ {
		if atomic.LoadInt32((*int32)(sw)) <= 0 {
			break
		}
		if i >= 1000 {
			i = 0
			time.Sleep(1 * time.Millisecond)
		}
	}
}
