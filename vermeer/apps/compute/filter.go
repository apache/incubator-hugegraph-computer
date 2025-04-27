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

package compute

import (
	"fmt"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

type VertexFilter struct {
	filter     *Filter
	properties structure.VertexPropertyInterface
}

func (v *VertexFilter) Init(exprStr string, propertyKeys []string, properties structure.VertexPropertyInterface) error {
	v.properties = properties
	initEnv := make(map[string]any, len(propertyKeys))
	for _, key := range propertyKeys {
		valueType, ok := v.properties.GetValueType(key)
		if !ok {
			logrus.Errorf("property %v not found in vertex", key)
			continue
		}
		switch valueType {
		case structure.ValueTypeInt32:
			initEnv[key] = 0
		case structure.ValueTypeFloat32:
			initEnv[key] = float32(0)
		case structure.ValueTypeString:
			initEnv[key] = ""
		}
	}
	v.filter = &Filter{}
	err := v.filter.Init(exprStr, initEnv)
	if err != nil {
		return err
	}
	_, err = v.filter.Do()
	if err != nil {
		return err
	}
	return nil
}

func (v *VertexFilter) Filter(vertexID uint32) bool {
	for key := range v.filter.env {
		var value any
		valueType, ok := v.properties.GetValueType(key)
		if !ok {
			logrus.Errorf("property %v not found in vertex", key)
			continue
		}
		switch valueType {
		case structure.ValueTypeInt32:
			value = int(v.properties.GetInt32Value(key, vertexID))
		case structure.ValueTypeFloat32:
			value = float32(v.properties.GetFloat32Value(key, vertexID))
		case structure.ValueTypeString:
			value = string(v.properties.GetStringValue(key, vertexID))
		}
		v.filter.env[key] = value
	}

	do, err := v.filter.Do()
	if err != nil {
		logrus.Errorf("fliter error:%v", err)
		return false
	}
	return do
}

type EdgeFilter struct {
	filter     *Filter
	properties structure.EdgesPropertyInterface
}

func (e *EdgeFilter) Init(exprStr string, propertyKeys []string, properties structure.EdgesPropertyInterface) error {
	e.properties = properties
	initEnv := make(map[string]any, len(propertyKeys))
	for _, key := range propertyKeys {
		valueType, ok := e.properties.GetValueType(key)
		if !ok {
			logrus.Errorf("property %v not found in edge", key)
			continue
		}
		switch valueType {
		case structure.ValueTypeInt32:
			initEnv[key] = 0
		case structure.ValueTypeFloat32:
			initEnv[key] = float32(0)
		case structure.ValueTypeString:
			initEnv[key] = ""
		}
	}
	e.filter = &Filter{}
	err := e.filter.Init(exprStr, initEnv)
	if err != nil {
		return err
	}
	return nil
}

func (e *EdgeFilter) Filter(vertID uint32, idx uint32) bool {
	for key := range e.filter.env {
		var value any
		valueType, ok := e.properties.GetValueType(key)
		if !ok {
			logrus.Errorf("property %v not found in edge", key)
			continue
		}
		switch valueType {
		case structure.ValueTypeInt32:
			value = int(e.properties.GetInt32Value(key, vertID, idx))
		case structure.ValueTypeFloat32:
			value = float32(e.properties.GetFloat32Value(key, vertID, idx))
		case structure.ValueTypeString:
			value = string(e.properties.GetStringValue(key, vertID, idx))
		}
		//value, err := e.properties.GetValues(key, vertID, idx)
		//if err != nil {
		//	logrus.Errorf("filter get values error:%v", err)
		//	return false
		//}
		e.filter.env[key] = value
	}

	do, err := e.filter.Do()
	if err != nil {
		logrus.Errorf("fliter error:%v", err)
		return false
	}
	return do
}

type Filter struct {
	program *vm.Program
	env     map[string]any
}

func (f *Filter) Init(exprStr string, initEnv map[string]any) error {
	f.env = initEnv
	var err error
	f.program, err = expr.Compile(exprStr, expr.Env(initEnv))
	if err != nil {
		return fmt.Errorf("filter expr compile error:%w", err)
	}
	return nil
}

func (f *Filter) Do() (bool, error) {
	run, err := expr.Run(f.program, f.env)
	if err != nil {
		return false, err
	}
	res, ok := run.(bool)
	if !ok {
		return false, fmt.Errorf("filter result not bool:%v", run)
	}
	return res, nil
}
