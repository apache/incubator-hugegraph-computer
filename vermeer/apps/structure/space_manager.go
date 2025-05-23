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

import (
	"errors"
	"sync"
)

var SpaceManager = &spaceManager{}

type spaceManager struct {
	MutexLocker
	spaces map[string]*GraphSpace
}

// AppendSpace Create a new space if the name does not exist.
func (sm *spaceManager) AppendSpace(name string) (*GraphSpace, error) {
	sm.locker.Lock()
	defer sm.locker.Unlock()
	if name == "" {
		return nil, errors.New("invalid space name")
	}
	if sm.spaces[name] != nil {
		return sm.spaces[name], nil
	}
	g := &GraphSpace{name: name}
	sm.spaces[name] = g
	return g, nil
}

func (sm *spaceManager) GetAllSpace() []*GraphSpace {
	spaces := make([]*GraphSpace, 0)
	for _, v := range sm.spaces {
		spaces = append(spaces, v)
	}
	return spaces
}

func (sm *spaceManager) GetGraphSpace(name string) *GraphSpace {
	return sm.spaces[name]
}

func (sm *spaceManager) Init() {
	sm.spaces = make(map[string]*GraphSpace)
}

func (sm *spaceManager) Locker() *sync.Mutex {
	return &sm.locker
}
