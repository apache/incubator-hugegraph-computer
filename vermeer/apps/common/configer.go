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
	"sync"
)

var stdConfig = new(Config)

type Config struct {
	data sync.Map
}

func (config *Config) Set(key string, value interface{}) {
	config.data.Store(key, value)
}

func (config *Config) Get(key string) interface{} {
	v, _ := config.data.Load(key)
	return v
}

func (config *Config) GetDefault(key string, dv interface{}) interface{} {
	v, ok := config.data.Load(key)
	if ok {
		return v
	}
	return dv
}

func ConfigerInit(src map[string]string) {
	if src == nil {
		return
	}
	for k, v := range src {
		SetConfig(k, v)
	}
}

func GetConfig(key string) interface{} {
	return stdConfig.Get(key)
}

func GetConfigDefault(key string, dv interface{}) interface{} {
	return stdConfig.GetDefault(key, dv)
}

func SetConfig(key string, value interface{}) {
	stdConfig.Set(key, value)
}
