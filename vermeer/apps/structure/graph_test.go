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
	"math"
	"reflect"
	"testing"
	"vermeer/apps/serialize"

	"github.com/sirupsen/logrus"
)

func buildGraphData(graphData *GraphData) {

	graphData.graphName = "testDb"
	graphData.VertIDStart = 1
	graphData.VertexCount = 100

	graphData.Vertex = &VertexMem{
		TotalVertex: []Vertex{{ID: "1"}, {ID: "2"}, {ID: "3"}},
		VertexLongIDMap: map[string]uint32{
			"test1": 1,
			"test2": 2,
			"test3": 3,
		},
	}

	graphData.Edges = &EdgeMem{
		useOutEdges:  true,
		useOutDegree: true,
		InEdges:      serialize.TwoDimSliceUint32{{1, 2, 3}, {3, 4, 5}, {6, 7, 8}},
		OutEdges:     serialize.TwoDimSliceUint32{{1, 2, 3}, {3, 4, 5}, {6, 7, 8}},
		OutDegree:    []serialize.SUint32{10, 11, 12},
		EdgeLocker:   nil,
	}

	schema := make([]*PSchema, 3)

	schemaInt := new(PSchema)
	schemaInt.PropKey = "schemaInt"
	schemaInt.VType = ValueTypeInt32
	schema[0] = schemaInt

	schemaString := new(PSchema)
	schemaString.PropKey = "schemaString"
	schemaString.VType = ValueTypeString
	schema[1] = schemaString

	schemaFloat := new(PSchema)
	schemaFloat.PropKey = "schemaFloat"
	schemaFloat.VType = ValueTypeFloat32
	schema[2] = schemaFloat

	var hugegraphPSchema HugegraphPSchema
	hugegraphPSchema.PropIndex = map[int]int{
		1111: 1,
		2222: 2,
		3333: 3,
	}
	hugegraphPSchema.Labels = map[int]string{
		1111: "test1",
		2222: "test2",
		3333: "test3",
	}

	graphData.VertexPropertySchema.Schema = schema
	graphData.VertexPropertySchema.HgPSchema = &hugegraphPSchema

	graphData.InEdgesPropertySchema.Schema = schema
	graphData.InEdgesPropertySchema.HgPSchema = &hugegraphPSchema

	vp := VertexProperties{}
	vp = make(map[string]*VValues, 3)

	vvi := new(VValues)
	vvi.VType = ValueTypeInt32
	vvi.Values = []serialize.SInt32{11, 22, 33}
	vp["vertex_property_int"] = vvi

	vvf := new(VValues)
	vvf.VType = ValueTypeFloat32
	vvf.Values = []serialize.SFloat32{11.0, 22.0, 33.0}
	vp["vertex_property_float"] = vvf

	vvs := new(VValues)
	vvs.VType = ValueTypeString
	vvs.Values = []serialize.SString{"vp11", "vp22", "vp33"}
	vp["vertex_property_string"] = vvs
	graphData.VertexProperty = &vp

	ep := EdgeProperties{}
	ep = make(map[string]*VValues, 3)

	epi := new(VValues)
	epi.VType = ValueTypeInt32
	epi.Values = [][]serialize.SInt32{{11, 22, 33}, {44, 55, 66}, {77, 88, 99}}
	ep["in_edges_property_int"] = epi

	epf := new(VValues)
	epf.VType = ValueTypeFloat32
	epf.Values = [][]serialize.SFloat32{{11.0, 22.0, 33.0}, {44.0, 55.0, 66.0}, {77.0, 88.0, 99.0}}
	ep["in_edges_property_float"] = epf

	eps := new(VValues)
	eps.VType = ValueTypeString
	eps.Values = [][]serialize.SString{{"ep11", "ep22", "ep33"}, {"ep44", "ep55", "ep66"}, {"ep77", "ep88", "ep99"}}
	ep["in_edges_property_string"] = eps

	graphData.InEdgesProperty = &ep

}

func TestVermeerGraph_GraphDataSave(t *testing.T) {

	var graphData GraphData

	buildGraphData(&graphData)

	var vg VermeerGraph
	vg.Name = "testDb"
	vg.SpaceName = "space0"
	vg.Data = &graphData
	err := vg.Save("1234")
	if err != nil {
		t.Fatalf("GraphDataSave error,err:%v", err)
	}

}

func TestVermeerGraph_GraphDataLoad(t *testing.T) {

	var graphData GraphData

	buildGraphData(&graphData)

	var vg VermeerGraph
	vg.Name = "testDb"
	vg.SpaceName = "space0"
	err := vg.Read("1234")
	if err != nil {
		t.Fatalf("GraphDataRead error,err:%v", err)
	}

	ok := IsEqual(&graphData, vg.Data)
	if !ok {
		t.Fatalf("isEqual error,result:%v", ok)
	}

}

func IsEqual(data1 *GraphData, data2 *GraphData) bool {

	if data1.graphName != data2.graphName {
		logrus.Errorf(" name is not equal:%v!=%v", data1.graphName, data2.graphName)
		return false
	}
	if data1.VertexCount != data2.VertexCount {
		logrus.Errorf(" VertexCount is not equal:%v!=%v", data1.VertexCount, data2.VertexCount)
		return false
	}

	if data1.VertIDStart != data2.VertIDStart {
		logrus.Errorf(" VertIDStart is not equal:%v!=%v", data1.VertIDStart, data2.VertIDStart)
		return false
	}

	if len(data1.Vertex.(*VertexMem).TotalVertex) != len(data2.Vertex.(*VertexMem).TotalVertex) {
		logrus.Errorf(" TotalVertex is not equal:%v!=%v", len(data1.Vertex.(*VertexMem).TotalVertex), len(data2.Vertex.(*VertexMem).TotalVertex))
		return false
	}

	for i, v := range data1.Vertex.(*VertexMem).TotalVertex {
		if v.ID != data2.Vertex.(*VertexMem).TotalVertex[i].ID {
			logrus.Errorf(" Vertex Id is not equal:%v!=%v", v.ID, data2.Vertex.(*VertexMem).TotalVertex[i].ID)
			return false
		}
	}

	for i, v := range data1.Edges.(*EdgeMem).InEdges {
		if len(v) != len(data2.Edges.(*EdgeMem).InEdges[i]) {
			logrus.Errorf(" InEdges length is not equal:%v!=%v", len(data1.Edges.(*EdgeMem).InEdges[i]), len(data2.Edges.(*EdgeMem).InEdges[i]))
			return false
		}
		for ii, vv := range v {
			if vv != data2.Edges.(*EdgeMem).InEdges[i][ii] {
				logrus.Errorf(" InEdges value is not equal:%v!=%v", vv, data2.Edges.(*EdgeMem).InEdges[i][ii])
				return false
			}
		}
	}

	for i, v := range data1.Edges.(*EdgeMem).OutEdges {
		if len(v) != len(data2.Edges.(*EdgeMem).OutEdges[i]) {
			logrus.Errorf(" OutEdges length is not equal:%v!=%v", len(data1.Edges.(*EdgeMem).OutEdges[i]), len(data2.Edges.(*EdgeMem).OutEdges[i]))
			return false
		}
		for ii, vv := range v {
			if vv != data2.Edges.(*EdgeMem).OutEdges[i][ii] {
				logrus.Errorf(" OutEdges value is not equal:%v!=%v", vv, data2.Edges.(*EdgeMem).OutEdges[i][ii])
				return false
			}
		}
	}

	if len(data1.Edges.(*EdgeMem).OutDegree) != len(data2.Edges.(*EdgeMem).OutDegree) {
		logrus.Errorf(" OutDegree is not equal:%v!=%v", len(data1.Edges.(*EdgeMem).OutDegree), len(data2.Edges.(*EdgeMem).OutDegree))
		return false
	}

	for i, v := range data1.Edges.(*EdgeMem).OutDegree {
		if v != data2.Edges.(*EdgeMem).OutDegree[i] {
			logrus.Errorf(" OutDegree value is not equal:%v!=%v", v, data2.Edges.(*EdgeMem).OutDegree[i])
			return false
		}
	}

	if len(data1.Vertex.(*VertexMem).VertexLongIDMap) != len(data2.Vertex.(*VertexMem).VertexLongIDMap) {
		logrus.Errorf("VertexLongIDMap length not equal, data1 length=%v,data2 length=%v",
			len(data1.Vertex.(*VertexMem).VertexLongIDMap), len(data2.Vertex.(*VertexMem).VertexLongIDMap))
		return false
	}

	for k, v := range data1.Vertex.(*VertexMem).VertexLongIDMap {
		if v2, ok := data2.Vertex.(*VertexMem).VertexLongIDMap[k]; !ok {
			logrus.Errorf("VertexLongIDMap data1 k not exist in data2,data1 k=%v,v=%v", k, v)
			return false
		} else if v != v2 {
			logrus.Errorf("VertexLongIDMap data1 v not equal data2 v,  data1 v=%v,data2 v=%v", v, v2)
			return false
		}
	}

	if len(*data1.VertexProperty.(*VertexProperties)) != len(*data2.VertexProperty.(*VertexProperties)) {
		logrus.Errorf("VertexProperty length not equal, data1 length=%v,data2 length=%v",
			len(*data1.VertexProperty.(*VertexProperties)), len(*data2.VertexProperty.(*VertexProperties)))
		return false
	}

	for k, v1 := range *data1.VertexProperty.(*VertexProperties) {
		if v2, ok := (*data2.VertexProperty.(*VertexProperties))[k]; !ok {
			logrus.Infof("VertexProperty data1 k not exist in data2, data1 k=%v", k)
			return false
		} else if !VValuesIsEqual(v1, v2) {
			logrus.Errorf("VertexProperty vvalues not equal,k=%v", k)
			return false
		}

	}

	if len(*data1.InEdgesProperty.(*EdgeProperties)) != len(*data2.InEdgesProperty.(*EdgeProperties)) {
		logrus.Debugf("InEdgesProperty length not equal, data1 length=%v,data2 length=%v",
			len(*data1.InEdgesProperty.(*EdgeProperties)), len(*data2.InEdgesProperty.(*EdgeProperties)))
		return false
	}

	for k, v1 := range *data1.InEdgesProperty.(*EdgeProperties) {
		if v2, ok := (*data2.InEdgesProperty.(*EdgeProperties))[k]; !ok {
			logrus.Errorf("InEdgesProperty data1 k not exist in data2, data1 k=%v", k)
			return false
		} else if !VValuesIsEqual(v1, v2) {
			logrus.Errorf("InEdgesProperty vvalues not equal,k=%v", k)
			return false
		}
	}
	return true
}

func VValuesIsEqual(v1 *VValues, v2 *VValues) bool {

	if v1.VType != v2.VType {
		logrus.Errorf("VValues VType not equal, v1 VType=%v,v2 VType=%v", v1.VType, v2.VType)
		return false
	}

	type1 := reflect.TypeOf(v1.Values)
	type2 := reflect.TypeOf(v2.Values)

	if type1 != type2 {
		logrus.Errorf("VValues type not equal, v1 VType=%v,v2 VType=%v", type1, type2)
		return false
	}

	switch v1.Values.(type) {
	case []serialize.SInt32:
		{
			vv1 := v1.Values.([]serialize.SInt32)
			vv2 := v2.Values.([]serialize.SInt32)

			return isEqualSliceSInt32(vv1, vv2)

		}
	case [][]serialize.SInt32:
		{
			vv1 := v1.Values.([][]serialize.SInt32)
			vv2 := v2.Values.([][]serialize.SInt32)

			return isEqualTwoDimSliceSInt32(vv1, vv2)

		}
	case []serialize.SFloat32:
		{
			vv1 := v1.Values.([]serialize.SFloat32)
			vv2 := v2.Values.([]serialize.SFloat32)
			return isEqualSliceSFloat32(vv1, vv2)

		}
	case [][]serialize.SFloat32:
		{
			vv1 := v1.Values.([][]serialize.SFloat32)
			vv2 := v2.Values.([][]serialize.SFloat32)

			return isEqualTwoDimSliceSFloat32(vv1, vv2)
		}
	case []serialize.SString:
		{
			vv1 := v1.Values.([]serialize.SString)
			vv2 := v2.Values.([]serialize.SString)

			return isEqualSliceSString(vv1, vv2)
		}
	case [][]serialize.SString:
		{
			vv1 := v1.Values.([][]serialize.SString)
			vv2 := v2.Values.([][]serialize.SString)
			return isEqualTwoDimSliceSString(vv1, vv2)
		}

	}
	return true
}

func isEqualSliceSInt32(vv1 []serialize.SInt32, vv2 []serialize.SInt32) bool {
	if len(vv1) != len(vv2) {
		logrus.Errorf("VValues []serialize.SInt32 length not equal, vv1 length=%v,vv2 length=%v", len(vv1), len(vv2))
		return false
	}
	for i := 0; i < len(vv1); i++ {
		if vv1[i] != vv2[i] {
			logrus.Errorf("VValues []serialize.SInt32 value not equal, vv1 value=%v,vv2 value=%v", vv1[i], vv2[i])
			return false
		}
	}
	return true
}

func isEqualTwoDimSliceSInt32(vv1 [][]serialize.SInt32, vv2 [][]serialize.SInt32) bool {

	if len(vv1) != len(vv2) {
		logrus.Errorf("VValues [][]serialize.SInt32 length not equal, vv1 length=%v,vv2 length=%v", len(vv1), len(vv2))
		return false
	}
	for i := 0; i < len(vv1); i++ {
		if len(vv1[i]) != len(vv2[i]) {
			logrus.Errorf("VValues [i][]serialize.SInt32 length not equal, vv1 length=%v,vv2 length=%v", len(vv1[i]), len(vv2[i]))
			return false
		}
		for j := 0; j < len(vv1[i]); j++ {
			if vv1[i][j] != vv2[i][j] {
				logrus.Errorf("VValues [i][j]serialize.SInt32 value not equal, vv1 value=%v,vv2 value=%v", len(vv1[i]), len(vv2[i]))
				return false
			}
		}
	}
	return true
}

func isEqualSliceSFloat32(vv1 []serialize.SFloat32, vv2 []serialize.SFloat32) bool {

	if len(vv1) != len(vv2) {
		logrus.Errorf("VValues []serialize.SFloat32 length not equal, vv1 length=%v,vv2 length=%v", len(vv1), len(vv2))
		return false
	}
	for i := 0; i < len(vv1); i++ {
		if math.Abs(float64(vv1[i]-vv2[i])) > 0.0001 {
			logrus.Errorf("VValues []serialize.SFloat32 value not equal, vv1 value=%v,vv2 value=%v", vv1[i], vv2[i])
			return false
		}
	}
	return true
}

func isEqualTwoDimSliceSFloat32(vv1 [][]serialize.SFloat32, vv2 [][]serialize.SFloat32) bool {

	if len(vv1) != len(vv2) {
		logrus.Errorf("VValues [][]serialize.SFloat32 length not equal, vv1 length=%v,vv2 length=%v", len(vv1), len(vv2))
		return false
	}
	for i := 0; i < len(vv1); i++ {
		if len(vv1[i]) != len(vv2[i]) {
			logrus.Errorf("VValues [i][]serialize.SFloat32 length not equal, vv1 length=%v,vv2 length=%v", len(vv1[i]), len(vv2[i]))
			return false
		}
		for j := 0; j < len(vv1[i]); j++ {
			if math.Abs(float64(vv1[i][j]-vv2[i][j])) > 0.0001 {
				logrus.Errorf("VValues [i][j]serialize.SFloat32 value not equal, vv1 value=%v,vv2 value=%v", len(vv1[i]), len(vv2[i]))
				return false
			}
		}
	}
	return true
}

func isEqualSliceSString(vv1 []serialize.SString, vv2 []serialize.SString) bool {

	if len(vv1) != len(vv2) {
		logrus.Errorf("VValues []serialize.SString length not equal, vv1 length=%v,vv2 length=%v", len(vv1), len(vv2))
		return false
	}
	for i := 0; i < len(vv1); i++ {
		if vv1[i] != vv2[i] {
			logrus.Errorf("VValues []serialize.SString value not equal, vv1 value=%v,vv2 value=%v", vv1[i], vv2[i])
			return false
		}
	}
	return true
}

func isEqualTwoDimSliceSString(vv1 [][]serialize.SString, vv2 [][]serialize.SString) bool {
	if len(vv1) != len(vv2) {
		logrus.Errorf("VValues [][]serialize.SString length not equal, vv1 length=%v,vv2 length=%v", len(vv1), len(vv2))
		return false
	}
	for i := 0; i < len(vv1); i++ {
		if len(vv1[i]) != len(vv2[i]) {
			logrus.Errorf("VValues [i][]serialize.SString length not equal, vv1 length=%v,vv2 length=%v", len(vv1[i]), len(vv2[i]))
			return false
		}
		for j := 0; j < len(vv1[i]); j++ {
			if vv1[i][j] != vv2[i][j] {
				logrus.Errorf("VValues [i][j]serialize.SString value not equal, vv1 value=%v,vv2 value=%v", len(vv1[i]), len(vv2[i]))
				return false
			}
		}
	}

	return true

}
