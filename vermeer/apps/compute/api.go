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
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"plugin"
	"strings"
	"vermeer/apps/buffer"
	"vermeer/apps/serialize"
	"vermeer/apps/structure"

	"github.com/sirupsen/logrus"
)

type ValueType int

const (
	ValueTypeFloat32 ValueType = iota
	ValueTypeInt32
	ValueTypeInt64
	ValueTypeSliceUint32
	ValueTypeSliceFloat32
	ValueTypeSliceInt64
)

const (
	CValueActionNormal = iota
	CValueActionAggregate
)

const (
	UseOutEdge   = "use_out_edge"
	UseOutDegree = "use_out_degree"
	UseProperty  = "use_property"
)

type AlgorithmType int

const (
	AlgorithmOLAP AlgorithmType = iota
	AlgorithmOLTP
)

type CValue struct {
	ValueType ValueType
	Action    byte
	Value     interface{}
}

func (cv *CValue) Marshal(buffer []byte) (int, error) {
	offset := 0
	buffer[offset] = byte(cv.ValueType)
	offset++
	buffer[offset] = cv.Action
	offset++
	switch cv.ValueType {
	case ValueTypeInt32:
		v := cv.Value.(serialize.SInt32)
		n, _ := v.Marshal(buffer[offset:])
		offset += n
	case ValueTypeInt64:
		v := cv.Value.(serialize.SInt64)
		n, _ := v.Marshal(buffer[offset:])
		offset += n
	case ValueTypeFloat32:
		v := cv.Value.(serialize.SFloat32)
		n, _ := v.Marshal(buffer[offset:])
		offset += n
	case ValueTypeSliceUint32:
		v := cv.Value.(serialize.SliceUint32)
		n, _ := v.Marshal(buffer[offset:])
		offset += n
	case ValueTypeSliceFloat32:
		v := cv.Value.(serialize.SliceFloat32)
		n, _ := v.Marshal(buffer[offset:])
		offset += n
	case ValueTypeSliceInt64:
		v := cv.Value.(serialize.SliceInt64)
		n, _ := v.Marshal(buffer[offset:])
		offset += n
	default:
		logrus.Errorf("Marshal value type not matched: %d", cv.ValueType)
	}

	return offset, nil
}

func (cv *CValue) Unmarshal(buffer []byte) (int, error) {
	offset := 0
	cv.ValueType = ValueType(buffer[offset])
	offset++
	cv.Action = buffer[offset]
	offset++

	switch cv.ValueType {
	case ValueTypeInt32:
		var vv serialize.SInt32
		n, _ := vv.Unmarshal(buffer[offset:])
		cv.Value = vv
		offset += n
	case ValueTypeInt64:
		var vv serialize.SInt64
		n, _ := vv.Unmarshal(buffer[offset:])
		cv.Value = vv
		offset += n
	case ValueTypeFloat32:
		var vv serialize.SFloat32
		n, _ := vv.Unmarshal(buffer[offset:])
		cv.Value = vv
		offset += n
	case ValueTypeSliceUint32:
		var vv serialize.SliceUint32
		n, _ := vv.Unmarshal(buffer[offset:])
		cv.Value = vv
		offset += n
	case ValueTypeSliceFloat32:
		var vv serialize.SliceFloat32
		n, _ := vv.Unmarshal(buffer[offset:])
		cv.Value = vv
		offset += n
	case ValueTypeSliceInt64:
		var vv serialize.SliceInt64
		n, _ := vv.Unmarshal(buffer[offset:])
		cv.Value = vv
		offset += n
	default:
		logrus.Errorf("Unmarshal value type not matched: %d", cv.ValueType)
	}

	return offset, nil
}

func (cv *CValue) ToString() string {
	var toString string
	switch cv.ValueType {
	case ValueTypeInt32:
		value := cv.Value.(serialize.SInt32)
		toString = value.ToString()
	case ValueTypeInt64:
		value := cv.Value.(serialize.SInt64)
		toString = value.ToString()
	case ValueTypeFloat32:
		value := cv.Value.(serialize.SFloat32)
		toString = value.ToString()
	case ValueTypeSliceUint32:
		value := cv.Value.(serialize.SliceUint32)
		toString = value.ToString()
	case ValueTypeSliceFloat32:
		value := cv.Value.(serialize.SFloat32)
		toString = value.ToString()
	case ValueTypeSliceInt64:
		value := cv.Value.(serialize.SliceInt64)
		toString = value.ToString()
	default:
		logrus.Errorf("Unmarshal value type not matched: %d", cv.ValueType)
	}
	return toString
}

func (cv *CValue) PredictSize() int {
	var predictSize int
	predictSize += 2
	switch cv.ValueType {
	case ValueTypeInt32:
		value := cv.Value.(serialize.SInt32)
		predictSize = value.PredictSize()
	case ValueTypeInt64:
		value := cv.Value.(serialize.SInt64)
		predictSize = value.PredictSize()
	case ValueTypeFloat32:
		value := cv.Value.(serialize.SFloat32)
		predictSize = value.PredictSize()
	case ValueTypeSliceUint32:
		value := cv.Value.(serialize.SliceUint32)
		predictSize = value.PredictSize()
	case ValueTypeSliceFloat32:
		value := cv.Value.(serialize.SliceFloat32)
		predictSize = value.PredictSize()
	case ValueTypeSliceInt64:
		value := cv.Value.(serialize.SliceInt64)
		predictSize = value.PredictSize()
	default:
		logrus.Errorf("Unmarshal value type not matched: %d", cv.ValueType)
	}
	return predictSize
}

type CContext struct {
	context.Context
	CValues map[string]*CValue
}

func (cc *CContext) CreateValue(name string, vType ValueType, action byte) {
	vv := CValue{
		ValueType: vType,
		Action:    action,
	}

	cc.CValues[name] = &vv
}

func (cc *CContext) SetValue(name string, value interface{}) {
	cc.CValues[name].Value = value
}

func (cc *CContext) GetValue(name string) interface{} {
	return cc.CValues[name].Value
}

func (cc *CContext) MarshalValues() map[string][]byte {
	cValues := make(map[string][]byte, len(cc.CValues))
	for k, v := range cc.CValues {
		aa := make([]byte, v.PredictSize()+64)
		n, _ := v.Marshal(aa)
		bb := make([]byte, n)
		copy(bb, aa)
		cValues[k] = bb
		// logrus.Infof("context marshal value: %s, %v", k, v.Value)
	}
	return cValues
}

func (cc *CContext) UnmarshalValues(values map[string][]byte) {
	for k, v := range cc.CValues {
		if data, ok := values[k]; ok {
			_, err := v.Unmarshal(data)
			if err != nil {
				logrus.Errorf("compute value unmarshal error: %s", err)
			}
			// logrus.Infof("context unmarshal value: %s, %v", k, v.Value)
		}
	}
}

type VValues struct {
	VType  ValueType
	Values interface{}
}

type CWContext struct {
	CContext
	Params      map[string]string
	Step        int32
	Parallel    int
	Workers     int
	GraphData   *structure.GraphData
	SendBuffers []buffer.EncodeBuffer
	WorkerIdx   int
	Output      bool
}

func (cwc *CWContext) MakeSendBuffer() {
	cwc.SendBuffers = make([]buffer.EncodeBuffer, cwc.Parallel)
	for i := range cwc.SendBuffers {
		cwc.SendBuffers[i].Init(2 * 1024 * 1024)
	}
}

func (cwc *CWContext) PartCount(total int) int {
	if cwc.WorkerIdx+1 == cwc.Workers {
		return total - (total/cwc.Workers)*(cwc.Workers-1)
	}

	return total / cwc.Workers
}

func (cwc *CWContext) PartStart(total int) int {
	return (total / cwc.Workers) * cwc.WorkerIdx
}

type CMContext struct {
	CContext
	Params        map[string]string
	Step          int32
	Parallel      int
	WorkerCValues map[string]map[string]*CValue
}

func (cmc *CMContext) AggregateValue() {
	cValues := make(map[string]*CValue)
	for _, v := range cmc.WorkerCValues {
		for vn, vv := range v {
			if vv.Action != CValueActionAggregate {
				continue
			}
			if cv, ok := cValues[vn]; !ok {
				cValues[vn] = &CValue{
					ValueType: vv.ValueType,
					Action:    vv.Action,
					Value:     vv.Value,
				}
			} else {
				switch cv.ValueType {
				case ValueTypeFloat32:
					cValues[vn].Value = cValues[vn].Value.(serialize.SFloat32) + vv.Value.(serialize.SFloat32)
				case ValueTypeInt32:
					cValues[vn].Value = cValues[vn].Value.(serialize.SInt32) + vv.Value.(serialize.SInt32)
				case ValueTypeInt64:
					cValues[vn].Value = cValues[vn].Value.(serialize.SInt64) + vv.Value.(serialize.SInt64)
				case ValueTypeSliceUint32:
					cValues[vn].Value = append(cValues[vn].Value.(serialize.SliceUint32),
						vv.Value.(serialize.SliceUint32)...)
				case ValueTypeSliceFloat32:
					cValues[vn].Value = append(cValues[vn].Value.(serialize.SliceFloat32),
						vv.Value.(serialize.SliceFloat32)...)
				case ValueTypeSliceInt64:
					cValues[vn].Value = append(cValues[vn].Value.(serialize.SliceInt64),
						vv.Value.(serialize.SliceInt64)...)
				default:
					logrus.Errorf("AggregateValue unknown value type: %d", cv.ValueType)
				}
			}
		}
	}
	for k, v := range cValues {
		cmc.CreateValue(k, v.ValueType, v.Action)
		cmc.SetValue(k, v.Value)
	}
}

type WorkerComputer interface {
	MakeContext(params map[string]string) *CWContext
	Init() error
	Scatters() []int
	ScatterValue(sIdx int, vIdx int) serialize.MarshalAble
	VertexValue(vertexId uint32) serialize.MarshalAble
	BeforeStep()
	Compute(vertexId uint32, pId int)
	AfterStep()
	Close()
	Output() []byte
	Context() *CWContext
	OutputValueType() string
}

type MasterComputer interface {
	MakeContext(params map[string]string) *CMContext
	Init() error
	BeforeStep()
	Compute() bool
	AfterStep()
	Close()
	Statistics() map[string]any
	Output([][]byte) any
	Context() *CMContext
}

type WorkerComputerBase struct {
	WContext *CWContext
}

func (wcb *WorkerComputerBase) MakeContext(params map[string]string) *CWContext {
	wcb.WContext = &CWContext{
		CContext: CContext{
			Context: context.Background(),
			CValues: make(map[string]*CValue),
		},
		Params: params,
	}
	return wcb.WContext
}

func (wcb *WorkerComputerBase) Context() *CWContext {
	return wcb.WContext
}

func (wcb *WorkerComputerBase) CreateVertexValue() VValues {
	return VValues{}
}

func (wcb *WorkerComputerBase) Init() error {
	return nil
}
func (wcb *WorkerComputerBase) Scatters() []int {
	return []int{}
}
func (wcb *WorkerComputerBase) ScatterValue(int, int) serialize.MarshalAble {
	return nil
}
func (wcb *WorkerComputerBase) BeforeStep() {
}
func (wcb *WorkerComputerBase) Compute(vertexID uint32, pID int) {
	_, _ = vertexID, pID
}
func (wcb *WorkerComputerBase) AfterStep() {
}
func (wcb *WorkerComputerBase) VertexValue(vertexId uint32) serialize.MarshalAble {
	_ = vertexId
	return nil
}
func (wcb *WorkerComputerBase) Close() {
}
func (wcb *WorkerComputerBase) Output() []byte {
	return nil
}
func (wcb *WorkerComputerBase) OutputValueType() string {
	return "TEXT"
}

type MasterComputerBase struct {
	MContext *CMContext
}

func (mcb *MasterComputerBase) MakeContext(params map[string]string) *CMContext {
	mcb.MContext = &CMContext{
		CContext: CContext{
			Context: context.Background(),
			CValues: make(map[string]*CValue),
		},
		Params:        params,
		WorkerCValues: make(map[string]map[string]*CValue),
	}
	return mcb.MContext
}

func (mcb *MasterComputerBase) Init() error {
	return nil
}
func (mcb *MasterComputerBase) BeforeStep() {
}
func (mcb *MasterComputerBase) Compute() bool {
	return true
}
func (mcb *MasterComputerBase) AfterStep() {
}
func (mcb *MasterComputerBase) Close() {
}
func (mcb *MasterComputerBase) Output([][]byte) any {
	return nil
}
func (mcb *MasterComputerBase) Statistics() map[string]any {
	return nil
}
func (mcb *MasterComputerBase) Context() *CMContext {
	return mcb.MContext
}

type AlgorithmMaker interface {
	CreateWorkerComputer() WorkerComputer
	CreateMasterComputer() MasterComputer
	Name() string
	// DataNeeded outEdge, outDegree, undirected
	DataNeeded() []string
	SupportedStatistics() map[StatisticsType]struct{}
	Type() AlgorithmType
}
type AlgorithmMakerBase struct {
}

func (a *AlgorithmMakerBase) CreateWorkerComputer() WorkerComputer {
	return &WorkerComputerBase{}
}
func (a *AlgorithmMakerBase) CreateMasterComputer() MasterComputer {
	return &MasterComputerBase{}
}
func (a *AlgorithmMakerBase) Name() string {
	return "base"
}
func (a *AlgorithmMakerBase) DataNeeded() []string {
	return []string{}
}
func (a *AlgorithmMakerBase) SupportedStatistics() map[StatisticsType]struct{} {
	return map[StatisticsType]struct{}{StatisticsTypeIgnore: {}}
}
func (a *AlgorithmMakerBase) Type() AlgorithmType {
	return AlgorithmOLAP
}

var AlgorithmManager = &algorithmManager{}

type algorithmManager struct {
	algorithms map[string]AlgorithmMaker
}

func (am *algorithmManager) Init() {
	am.algorithms = make(map[string]AlgorithmMaker)
}

func (am *algorithmManager) Register(maker AlgorithmMaker, from string) {
	am.algorithms[maker.Name()] = maker
	logrus.Infof("Algorithm register OK, %s, from: %s", maker.Name(), from)
}

func (am *algorithmManager) GetMaker(algoName string) AlgorithmMaker {
	if maker, ok := am.algorithms[algoName]; ok {
		return maker
	}
	logrus.Errorf("GetMaker error, algo not exists: %s", algoName)
	return nil
}

func (am *algorithmManager) MakeWorkerComputer(algoName string) WorkerComputer {
	if maker, ok := am.algorithms[algoName]; ok {
		return maker.CreateWorkerComputer()
	}
	logrus.Errorf("MakeWorkerComputer error, algo not exists: %s", algoName)
	return nil
}

func (am *algorithmManager) MakeMasterComputer(algoName string) (MasterComputer, error) {
	if maker, ok := am.algorithms[algoName]; ok {
		return maker.CreateMasterComputer(), nil
	}
	logrus.Errorf("MakeMasterComputer error, algo not exists: %s", algoName)
	return nil, fmt.Errorf("MakeMasterComputer error, algo not exists: %s", algoName)
}

func (am *algorithmManager) LoadPlugins() {
	projectPath, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	pluginPath := filepath.Join(projectPath, "plugins/")
	_ = filepath.Walk(pluginPath, func(path string, info fs.FileInfo, err error) error {
		if info == nil {
			return nil
		}
		if !strings.HasSuffix(info.Name(), ".so") {
			return nil
		}

		plug, err := plugin.Open(path)
		if err != nil {
			logrus.Warnf("open plugin error: %s, %s", path, err)
			return nil
		}

		makerPlug, err := plug.Lookup("AlgoMaker")
		if err != nil {
			logrus.Warnf("plugin lookup error: %s, %s", path, err)
			return nil
		}

		maker, ok := makerPlug.(AlgorithmMaker)
		if !ok {
			logrus.Warnf("unexpected type from module symbol")
		}

		am.Register(maker, "plugin")

		return nil
	})
}
