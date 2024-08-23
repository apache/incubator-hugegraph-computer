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

package store

import (
	"path"
	"vermeer/apps/common"
	storage "vermeer/apps/storage"

	"github.com/sirupsen/logrus"
)

var ServiceStore = &serviceStore{}

type serviceStore struct {
	store storage.Store
}

func (ss *serviceStore) Init() error {
	p, err := common.GetCurrentPath()
	if err != nil {
		logrus.Errorf("get current path error:%v", err)
		return err
	}
	dir := path.Join(p, "vermeer_data", "service_info")
	ss.store, err = storage.StoreMaker(storage.StoreOption{
		StoreName: storage.StoreTypePebble,
		Path:      dir,
		Fsync:     true,
	})
	if err != nil {
		return err
	}
	return nil
}

func (ss *serviceStore) SaveDispatchPause(isPaused bool) error {
	b := byte(0)
	if isPaused {
		b = 1
	}
	return ss.store.Set([]byte("is_dispatch_paused"), []byte{b})
}

func (ss *serviceStore) GetDispatchPause() bool {
	if v, err := ss.store.Get([]byte("is_dispatch_paused")); err != nil {
		return false
	} else {
		if v[0] == 1 {
			return true
		}
		return false
	}
}
