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

package buffer

import (
	"fmt"
	"sync"
)

type DoubleBuffer struct {
	buffer     [][]byte
	current    int
	offset     int
	bufferSize int
	locker     sync.Mutex
}

func (db *DoubleBuffer) Init(size int) {
	db.buffer = make([][]byte, 2)
	db.buffer[0] = make([]byte, size)
	db.buffer[1] = make([]byte, size)
	db.current = 0
	db.locker = sync.Mutex{}
	db.bufferSize = size
	db.offset = 0
}

func (db *DoubleBuffer) Write(src []byte) error {
	db.locker.Lock()
	defer db.locker.Unlock()
	if len(src) > db.bufferSize {
		return fmt.Errorf("write error, too many src bytes, %d>%d", len(src), db.bufferSize)
	}
	if len(src) < (db.bufferSize - db.offset) {
		copy(db.buffer[db.current][db.offset:], src)
		db.offset += len(src)
	} else {
		db.current = (db.current + 1) % 2
		db.offset = len(src)
		copy(db.buffer[db.current], src)
	}
	return nil
}
