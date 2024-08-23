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

package structure

import "sync"

type MutexLocker struct {
	locker sync.Mutex `json:"-"`
}

func (ml *MutexLocker) Lock() any {
	ml.locker.Lock()
	return nil
}

func (ml *MutexLocker) Unlock(any) {
	ml.locker.Unlock()
}

type SyncAble interface {
	Sync() any
	UnSync(any)
}

type Syncer struct {
	SyncAble `json:"-"`
	sync     sync.Mutex `json:"-"`
}

func (s *Syncer) Sync() any {
	s.sync.Lock()
	return nil
}

func (s *Syncer) UnSync(any) {
	s.sync.Unlock()
}
