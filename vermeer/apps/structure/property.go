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
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"vermeer/apps/common"
	"vermeer/apps/options"
	store "vermeer/apps/protos/hugegraph-store-grpc"
	"vermeer/apps/serialize"

	"github.com/sirupsen/logrus"
)

const (
	ValueTypeFloat32 ValueType = iota
	ValueTypeInt32
	ValueTypeString
	ValueTypeUnknow
)

type ValueType uint16

type PSchema struct {
	VType   ValueType
	PropKey string
}

type HugegraphPSchema struct {
	//key is the hugegraph property_id, value is the vermeer property_id
	PropIndex map[int]int
	//key is the hugegraph label_id, value is hugegraph label_name
	Labels map[int]string
}

type PropertySchema struct {
	Schema    []*PSchema
	HgPSchema *HugegraphPSchema
}

func (ps *PropertySchema) Init(schema map[string]string) {
	ps.Schema = make([]*PSchema, len(schema))
	for k, v := range schema {

		vs := strings.Split(v, ",")
		if len(vs) < 2 {
			logrus.Errorf("Schema string Length Not enough,key:%v ", k)
			continue
		}
		vType, err := strconv.Atoi(vs[0])
		if err != nil {
			return
		}
		index, err := strconv.Atoi(vs[1])
		if err != nil {
			return
		}
		if index >= len(schema) {
			logrus.Errorf("Schema index out of range,index:%v", index)
			continue
		}
		ps.Schema[index] = &PSchema{}
		ps.Schema[index].PropKey = k
		ps.Schema[index].VType = ValueType(vType)
	}
}

func (ps *PropertySchema) initFromHugegraphVertex(hgSchema map[string]any, useProperty bool, vertexPropIDs []string) error {
	//从hugegraph中读取schema
	//edge和vertex的label和property不相同，需要分开读取。
	ps.HgPSchema = &HugegraphPSchema{}
	vertexPropIDMap := make(map[int]struct{})
	useVertexPropFilter := len(vertexPropIDs) > 0
	if useVertexPropFilter {
		useVertexPropFilter = false
		for _, propID := range vertexPropIDs {
			if propID == "" {
				continue
			}
			iLabel, err := strconv.ParseInt(strings.TrimSpace(propID), 10, 64)
			if err != nil {
				logrus.Errorf("property schema label type not int :%v", propID)
				continue
			} else {
				useVertexPropFilter = true
			}
			vertexPropIDMap[int(iLabel)] = struct{}{}
		}
	}
	vertexLabels, ok := hgSchema["vertexlabels"].([]any)
	if !ok {
		return fmt.Errorf("get vertexlabels from hugegraph not correct %v", hgSchema["vertexlabels"])
	}
	ps.HgPSchema.Labels = make(map[int]string)
	properties := make(map[string]struct{})
	for _, vLabelAny := range vertexLabels {
		vLabel, ok := vLabelAny.(map[string]any)
		if !ok {
			return fmt.Errorf("get vertex label not correct %v", vLabelAny)
		}
		id, ok := vLabel["id"].(float64)
		if !ok {
			return fmt.Errorf("get vertex label id not correct %v", vLabel["id"])
		}
		name, ok := vLabel["name"].(string)
		if !ok {
			return fmt.Errorf("get vertex label name not correct %v", vLabel["id"])
		}
		ps.HgPSchema.Labels[int(id)] = name
		labelProperties, ok := vLabel["properties"].([]any)
		if !ok {
			return fmt.Errorf("get vertex label properties not correct %v", vLabel["properties"])
		}
		for _, labelProperty := range labelProperties {
			propString, ok := labelProperty.(string)
			if !ok {
				return fmt.Errorf("get vertex label property not correct %v", labelProperty)
			}
			properties[propString] = struct{}{}
		}
	}
	if !useProperty {
		ps.Schema = make([]*PSchema, 1)
		ps.Schema[0] = &PSchema{
			VType:   ValueTypeString,
			PropKey: "label",
		}
	} else {
		propKeys, ok := hgSchema["propertykeys"].([]any)
		if !ok {
			return fmt.Errorf("get propertykeys from hugegraph not correct %v", hgSchema["propertykeys"])
		}
		ps.Schema = make([]*PSchema, 1, len(properties)+1)
		ps.Schema[0] = &PSchema{
			VType:   ValueTypeString,
			PropKey: "label",
		}
		ps.HgPSchema.PropIndex = make(map[int]int, len(properties))
		idx := 1
		for _, propKeyAny := range propKeys {
			propKey, ok := propKeyAny.(map[string]any)
			if !ok {
				return fmt.Errorf("get property key not correct %v", propKeyAny)
			}
			propKeyName, ok := propKey["name"].(string)
			if !ok {
				return fmt.Errorf("get property name not correct %v", propKey["name"])
			}
			if _, ok := properties[propKeyName]; !ok {
				continue
			}
			propID, ok := propKey["id"].(float64)
			if !ok {
				return fmt.Errorf("get property id not correct %v", propKey["id"])
			}
			if _, ok := vertexPropIDMap[int(propID)]; useVertexPropFilter && !ok {
				continue
			}
			ps.Schema = append(ps.Schema, &PSchema{})
			ps.Schema[idx].PropKey = propKeyName
			switch propKey["data_type"].(string) {
			case "INT", "LONG":
				ps.Schema[idx].VType = ValueTypeInt32
			case "FLOAT", "DOUBLE":
				ps.Schema[idx].VType = ValueTypeFloat32
			case "STRING", "BYTE", "DATE", "BOOL", "TEXT", "BOOLEAN":
				ps.Schema[idx].VType = ValueTypeString
			default:
				logrus.Errorf("hugegraph data_type:%v not match", propKey["data_type"].(string))
			}
			ps.HgPSchema.PropIndex[int(propID)] = idx
			idx++
		}
	}
	return nil
}

func (ps *PropertySchema) initFromHugegraphEdge(hgSchema map[string]any, useProperty bool, edgePropIDs []string) error {
	if !useProperty {
		return nil
	}
	ps.HgPSchema = &HugegraphPSchema{}
	edgePropIDMap := make(map[int]struct{})
	useEdgePropFilter := len(edgePropIDs) > 0
	if useEdgePropFilter {
		useEdgePropFilter = false
		for _, propID := range edgePropIDs {
			if propID == "" {
				continue
			}
			iLabel, err := strconv.ParseInt(strings.TrimSpace(propID), 10, 64)
			if err != nil {
				logrus.Errorf("property schema label type not int :%v", propID)
				continue
			} else {
				useEdgePropFilter = true
			}
			edgePropIDMap[int(iLabel)] = struct{}{}
		}
	}
	edgeLabels, ok := hgSchema["edgelabels"].([]any)
	if !ok {
		return fmt.Errorf("get edgelabels from hugegraph not correct %v", hgSchema["edgelabels"])
	}
	ps.HgPSchema.Labels = make(map[int]string)
	properties := make(map[string]struct{})
	for _, eLabelAny := range edgeLabels {
		eLabel, ok := eLabelAny.(map[string]any)
		if !ok {
			return fmt.Errorf("get edge label not correct %v", eLabelAny)
		}
		id, ok := eLabel["id"].(float64)
		if !ok {
			return fmt.Errorf("get edge label id not correct %v", eLabel["id"])
		}
		name, ok := eLabel["name"].(string)
		if !ok {
			return fmt.Errorf("get edge label name not correct %v", eLabel["id"])
		}
		edgeLabelType, ok := eLabel["edgelabel_type"].(string)
		if !ok {
			return fmt.Errorf("get edgelabel_type not correct %v", eLabel["edgelabel_type"])
		}
		if edgeLabelType == "PARENT" {
			continue
		}
		ps.HgPSchema.Labels[int(id)] = name
		labelProperties, ok := eLabel["properties"].([]any)
		if !ok {
			return fmt.Errorf("get edge label properties not correct %v", eLabel["properties"])
		}
		for _, labelProperty := range labelProperties {
			propString, ok := labelProperty.(string)
			if !ok {
				return fmt.Errorf("get edge label property not correct %v", labelProperty)
			}
			properties[propString] = struct{}{}
		}
	}
	propKeys, ok := hgSchema["propertykeys"].([]any)
	if !ok {
		return fmt.Errorf("get propertykeys from hugegraph not correct %v", hgSchema["propertykeys"])
	}
	ps.Schema = make([]*PSchema, 1, len(properties)+1)
	ps.Schema[0] = &PSchema{
		VType:   ValueTypeString,
		PropKey: "label",
	}
	ps.HgPSchema.PropIndex = make(map[int]int, len(properties))
	idx := 1
	for _, propKeyAny := range propKeys {
		propKey, ok := propKeyAny.(map[string]any)
		if !ok {
			return fmt.Errorf("get property key not correct %v", propKeyAny)
		}
		propKeyName, ok := propKey["name"].(string)
		if !ok {
			return fmt.Errorf("get property name not correct %v", propKey["name"])
		}
		if _, ok := properties[propKeyName]; !ok {
			continue
		}
		propID, ok := propKey["id"].(float64)
		if !ok {
			return fmt.Errorf("get property id not correct %v", propKey["id"])
		}
		if _, ok := edgePropIDMap[int(propID)]; useEdgePropFilter && !ok {
			continue
		}
		ps.Schema = append(ps.Schema, &PSchema{})
		ps.Schema[idx].PropKey = propKeyName
		switch propKey["data_type"].(string) {
		case "INT", "LONG":
			ps.Schema[idx].VType = ValueTypeInt32
		case "FLOAT", "DOUBLE":
			ps.Schema[idx].VType = ValueTypeFloat32
		case "STRING", "BYTE", "DATE", "BOOL", "TEXT", "BOOLEAN":
			ps.Schema[idx].VType = ValueTypeString
		default:
			logrus.Errorf("hugegraph data_type:%v not match", propKey["data_type"].(string))
		}
		ps.HgPSchema.PropIndex[int(propID)] = idx
		idx++
	}
	return nil
}

func (ps *PropertySchema) Marshal(buffer []byte) (int, error) {

	offset := 0
	schemaCount := len(ps.Schema)
	binary.BigEndian.PutUint32(buffer[offset:], uint32(schemaCount))
	offset += 4
	for i := 0; i < schemaCount; i++ {
		binary.BigEndian.PutUint16(buffer[offset:], uint16(ps.Schema[i].VType))
		offset += 2
		propKey := serialize.SString(ps.Schema[i].PropKey)
		n, err := propKey.Marshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}

	if ps.HgPSchema == nil {
		binary.BigEndian.PutUint32(buffer[offset:], 0)
		offset += 4
		return offset, nil
	}
	binary.BigEndian.PutUint32(buffer[offset:], 1)
	offset += 4

	hgPSchemaPropIndexCount := len(ps.HgPSchema.PropIndex)
	binary.BigEndian.PutUint32(buffer[offset:], uint32(hgPSchemaPropIndexCount))
	offset += 4

	for k, v := range ps.HgPSchema.PropIndex {
		kk := serialize.SInt32(k)
		n, err := kk.Marshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
		vv := serialize.SInt32(v)
		n, err = vv.Marshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}

	hgPSchemaLabelsCount := len(ps.HgPSchema.Labels)
	binary.BigEndian.PutUint32(buffer[offset:], uint32(hgPSchemaLabelsCount))
	offset += 4

	for k, v := range ps.HgPSchema.Labels {
		kk := serialize.SInt32(k)
		n, err := kk.Marshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n

		vv := serialize.SString(v)
		n, err = vv.Marshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}

	return offset, nil
}

func (ps *PropertySchema) Unmarshal(buffer []byte) (int, error) {

	offset := 0
	schemaCount := binary.BigEndian.Uint32(buffer[offset:])
	offset += 4
	(*ps).Schema = make([]*PSchema, schemaCount)

	for i := range (*ps).Schema {
		vType := ValueType(binary.BigEndian.Uint16(buffer[offset:]))
		offset += 2
		var propKey serialize.SString
		n, err := propKey.Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n

		schema := new(PSchema)
		schema.VType = vType
		schema.PropKey = string(propKey)
		(*ps).Schema[i] = schema
	}

	hasHgPSchema := binary.BigEndian.Uint32(buffer[offset:]) == 1
	offset += 4
	if !hasHgPSchema {
		return offset, nil
	}

	hgPSchemaPropIndexCount := binary.BigEndian.Uint32(buffer[offset:])
	offset += 4

	hgPSchema := new(HugegraphPSchema)
	hgPSchema.PropIndex = make(map[int]int, hgPSchemaPropIndexCount)
	for i := 0; i < int(hgPSchemaPropIndexCount); i++ {
		var k serialize.SInt32
		n, err := k.Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
		var v serialize.SInt32
		n, err = v.Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
		hgPSchema.PropIndex[int(k)] = int(v)
	}

	hgPSchemaLabelsCount := binary.BigEndian.Uint32(buffer[offset:])
	offset += 4

	hgPSchema.Labels = make(map[int]string, hgPSchemaLabelsCount)
	for i := 0; i < int(hgPSchemaLabelsCount); i++ {
		var k serialize.SInt32
		n, err := k.Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n

		var v serialize.SString
		n, err = v.Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
		hgPSchema.Labels[int(k)] = string(v)
	}

	ps.HgPSchema = hgPSchema

	return offset, nil
}

func (ps *PropertySchema) ToString() string {
	return ""
}

func (ps *PropertySchema) PredictSize() int {

	size := 0
	size += 4
	for _, v := range ps.Schema {
		size += 2
		propKey := serialize.SString(v.PropKey)
		size += propKey.PredictSize()
	}

	size += 4
	if ps.HgPSchema == nil {
		return size
	}

	size += len(ps.HgPSchema.PropIndex) * 8
	size += 4
	for _, v := range ps.HgPSchema.Labels {
		size += 4
		vv := serialize.SString(v)
		size += vv.PredictSize()
	}

	size += 4

	return size
}

func GetSchemaFromHugegraph(params map[string]string) (PropertySchema, PropertySchema, error) {
	pdIPAddress := options.GetSliceString(params, "load.hg_pd_peers")
	hgName := options.GetString(params, "load.hugegraph_name")
	username := options.GetString(params, "load.hugegraph_username")
	password := options.GetString(params, "load.hugegraph_password")
	useProperty := options.GetInt(params, "load.use_property") == 1
	vertexPropIDs := strings.Split(options.GetString(params, "load.vertex_property"), ",")
	edgePropIDs := strings.Split(options.GetString(params, "load.edge_property"), ",")
	logrus.Infof("vertex props:%v , edge props:%v", vertexPropIDs, edgePropIDs)
	pdAddr, err := common.FindValidPD(pdIPAddress)
	if err != nil {
		logrus.Errorf("find valid pd failed, %v", err.Error())
		return PropertySchema{}, PropertySchema{}, err
	}
	serverAddr, err := common.FindServerAddr(pdAddr, hgName, username, password)
	if err != nil {
		logrus.Errorf("find server addr failed, %v", err.Error())
		return PropertySchema{}, PropertySchema{}, err
	}

	propertyFromHg, err := common.GetHugegraphSchema(serverAddr, hgName, username, password)
	if err != nil {
		logrus.Errorf("get hugegraph schema failed, %v", err.Error())
		return PropertySchema{}, PropertySchema{}, err
	}
	vertexSchema := PropertySchema{}
	edgeSchema := PropertySchema{}
	err = vertexSchema.initFromHugegraphVertex(propertyFromHg, useProperty, vertexPropIDs)
	if err != nil {
		logrus.Errorf("init vertex schema failed, %v", err.Error())
		return PropertySchema{}, PropertySchema{}, err
	}
	err = edgeSchema.initFromHugegraphEdge(propertyFromHg, useProperty, edgePropIDs)
	if err != nil {
		logrus.Errorf("init edge schema failed, %v", err.Error())
		return PropertySchema{}, PropertySchema{}, err
	}
	return vertexSchema, edgeSchema, nil
}

type PropertyValue []serialize.MarshalAble

func (p *PropertyValue) Init(schema PropertySchema) {
	*p = make([]serialize.MarshalAble, len(schema.Schema))
	for i, v := range schema.Schema {
		switch v.VType {
		case ValueTypeInt32:
			value := serialize.SInt32(0)
			(*p)[i] = &value
		case ValueTypeFloat32:
			value := serialize.SFloat32(0)
			(*p)[i] = &value
		case ValueTypeString:
			value := serialize.SString("")
			(*p)[i] = &value
		default:
			logrus.Errorf("PropertyValue Init err, No matching type:%v", v.VType)
		}
	}
}

func (p *PropertyValue) Marshal(buffer []byte) (int, error) {
	offset := 0
	binary.BigEndian.PutUint32(buffer, uint32(len(*p)))
	offset += 4
	for _, v := range *p {
		n, err := v.Marshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	return offset, nil
}

func (p *PropertyValue) Unmarshal(buffer []byte) (int, error) {
	offset := 0
	length := binary.BigEndian.Uint32(buffer)
	offset += 4
	for i := 0; i < int(length); i++ {
		n, err := (*p)[i].Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	return offset, nil
}

func (p *PropertyValue) ToString() string {
	//TODO
	return ""
}

func (p *PropertyValue) PredictSize() int {
	return 0
}

func (p *PropertyValue) CopyProperty(schema PropertySchema) PropertyValue {
	newPropertyValue := make([]serialize.MarshalAble, len(*p))
	for i, v := range schema.Schema {
		switch v.VType {
		case ValueTypeInt32:
			value := *(*p)[i].(*serialize.SInt32)
			newPropertyValue[i] = &value
		case ValueTypeFloat32:
			value := *(*p)[i].(*serialize.SFloat32)
			newPropertyValue[i] = &value
		case ValueTypeString:
			value := *(*p)[i].(*serialize.SString)
			newPropertyValue[i] = &value
		default:
			logrus.Errorf("PropertyValue Init err, No matching type:%v", v.VType)
		}
	}
	return newPropertyValue
}

func (p *PropertyValue) LoadFromString(str string, schema PropertySchema) {
	strs := make([]string, 0)
	hasQuotationMark := false
	split := 0
	for i := range str {
		if !hasQuotationMark && str[i] == ',' {
			strs = append(strs, strings.TrimSpace(str[split:i]))
			split = i + 1
		}
		if str[i] == '"' {
			hasQuotationMark = !hasQuotationMark
		}
	}
	if split < len(str) {
		strs = append(strs, strings.TrimSpace(str[split:]))
	}

	for i, v := range schema.Schema {
		if i >= len(strs) {
			continue
		}
		switch v.VType {
		case ValueTypeInt32:
			n, err := strconv.Atoi(strs[i])
			if err != nil {
				logrus.Errorf("Property LoadFromString Atoi err:%v", err)
				continue
			}
			value := serialize.SInt32(n)
			(*p)[i] = &value
		case ValueTypeFloat32:
			n, err := strconv.ParseFloat(strs[i], 32)
			if err != nil {
				logrus.Errorf("Property LoadFromString ParseFloat err:%v", err)
				continue
			}
			value := serialize.SFloat32(n)
			(*p)[i] = &value
		case ValueTypeString:
			value := serialize.SString(strs[i])
			(*p)[i] = &value
		}
	}
}

func (p *PropertyValue) LoadFromHugegraph(prop []*store.Property, schema PropertySchema) {
	for _, prop := range prop {
		index, ok := schema.HgPSchema.PropIndex[int(prop.Label)]
		if !ok {
			logrus.Warnf("property index not exist:%v", prop.Label)
			continue
		}
		switch schema.Schema[index].VType {
		case ValueTypeInt32:
			valueInt, err := common.VariantToInt(prop.Value)
			if err != nil {
				logrus.Infof("VariantToInt error:%v", err)
			}
			value := serialize.SInt32(valueInt)
			(*p)[index] = &value
		case ValueTypeFloat32:
			valueFloat, err := common.VariantToFloat(prop.Value)
			if err != nil {
				logrus.Infof("VariantToFloat error:%v", err)
			}
			value := serialize.SFloat32(valueFloat)
			(*p)[index] = &value
		case ValueTypeString:
			valueString, err := common.VariantToString(prop.Value)
			if err != nil {
				logrus.Infof("VariantToString error:%v", err)
			}
			value := serialize.SString(valueString)
			(*p)[index] = &value
		}
	}
}

type VValues struct {
	VType  ValueType
	Values interface{}
}

type VertexProperties map[string]*VValues

func (vp *VertexProperties) Init(schema PropertySchema) {
	*vp = make(map[string]*VValues, len(schema.Schema))
	for _, v := range schema.Schema {
		(*vp)[v.PropKey] = &VValues{}
		(*vp)[v.PropKey].VType = v.VType
		switch v.VType {
		case ValueTypeInt32:
			(*vp)[v.PropKey].Values = make([]serialize.SInt32, 0)
		case ValueTypeFloat32:
			(*vp)[v.PropKey].Values = make([]serialize.SFloat32, 0)
		case ValueTypeString:
			(*vp)[v.PropKey].Values = make([]serialize.SString, 0)
		}
	}
}
func (vp *VertexProperties) AppendProp(prop PropertyValue, schema PropertySchema) {
	values := prop.CopyProperty(schema)
	for i, v := range schema.Schema {
		switch v.VType {
		case ValueTypeInt32:
			(*vp)[v.PropKey].Values = append((*vp)[v.PropKey].Values.([]serialize.SInt32),
				*values[i].(*serialize.SInt32))
		case ValueTypeFloat32:
			(*vp)[v.PropKey].Values = append((*vp)[v.PropKey].Values.([]serialize.SFloat32),
				*values[i].(*serialize.SFloat32))
		case ValueTypeString:
			(*vp)[v.PropKey].Values = append((*vp)[v.PropKey].Values.([]serialize.SString),
				*values[i].(*serialize.SString))
		}

	}
}

func (vp *VertexProperties) AppendProps(prop VertexProperties) {
	for k := range *vp {
		switch (*vp)[k].VType {
		case ValueTypeInt32:
			(*vp)[k].Values = append((*vp)[k].Values.([]serialize.SInt32),
				prop[k].Values.([]serialize.SInt32)...)
		case ValueTypeFloat32:
			(*vp)[k].Values = append((*vp)[k].Values.([]serialize.SFloat32),
				prop[k].Values.([]serialize.SFloat32)...)
		case ValueTypeString:
			(*vp)[k].Values = append((*vp)[k].Values.([]serialize.SString),
				prop[k].Values.([]serialize.SString)...)
		}
	}
}

func (vp *VertexProperties) GetValueType(propKey string) (ValueType, bool) {
	prop, ok := (*vp)[propKey]
	return prop.VType, ok
}

func (vp *VertexProperties) GetValue(propKey string, idx uint32) serialize.MarshalAble {
	var value serialize.MarshalAble
	switch (*vp)[propKey].VType {
	case ValueTypeInt32:
		value = &(*vp)[propKey].Values.([]serialize.SInt32)[idx]
	case ValueTypeFloat32:
		value = &(*vp)[propKey].Values.([]serialize.SFloat32)[idx]
	case ValueTypeString:
		value = &(*vp)[propKey].Values.([]serialize.SString)[idx]
	}
	return value
}

func (vp *VertexProperties) SetValue(propKey string, idx uint32, value serialize.MarshalAble) {
	switch (*vp)[propKey].VType {
	case ValueTypeInt32:
		sInt32 := value.(*serialize.SInt32)
		(*vp)[propKey].Values.([]serialize.SInt32)[idx] = *sInt32
	case ValueTypeFloat32:
		sFloat32 := value.(*serialize.SFloat32)
		(*vp)[propKey].Values.([]serialize.SFloat32)[idx] = *sFloat32
	case ValueTypeString:
		sString := value.(*serialize.SString)
		(*vp)[propKey].Values.([]serialize.SString)[idx] = *sString
	}
}

func (vp *VertexProperties) GetInt32Value(propKey string, idx uint32) serialize.SInt32 {
	value := (*vp)[propKey].Values.([]serialize.SInt32)[idx]
	return value
}

func (vp *VertexProperties) GetStringValue(propKey string, idx uint32) serialize.SString {
	value := (*vp)[propKey].Values.([]serialize.SString)[idx]
	return value
}

func (vp *VertexProperties) GetFloat32Value(propKey string, idx uint32) serialize.SFloat32 {
	value := (*vp)[propKey].Values.([]serialize.SFloat32)[idx]
	return value
}

func (vp *VertexProperties) GetValues(propKey string, idx uint32) (any, error) {
	var value any
	_, ok := (*vp)[propKey]
	// if idx < 0 {
	// 	return nil, fmt.Errorf("idx:%v out of range", idx)
	// }
	if !ok {
		return nil, fmt.Errorf("property key:%v not exist", propKey)
	}
	switch (*vp)[propKey].VType {
	case ValueTypeInt32:
		if int(idx) >= len((*vp)[propKey].Values.([]serialize.SInt32)) {
			return nil, fmt.Errorf("idx:%v out of range", idx)
		}
		value = (*vp)[propKey].Values.([]serialize.SInt32)[idx]
	case ValueTypeFloat32:
		if int(idx) >= len((*vp)[propKey].Values.([]serialize.SFloat32)) {
			return nil, fmt.Errorf("idx:%v out of range", idx)
		}
		value = (*vp)[propKey].Values.([]serialize.SFloat32)[idx]
	case ValueTypeString:
		if int(idx) >= len((*vp)[propKey].Values.([]serialize.SString)) {
			return nil, fmt.Errorf("idx:%v out of range", idx)
		}
		value = (*vp)[propKey].Values.([]serialize.SString)[idx]
	}
	return value, nil
}

func (vp *VertexProperties) Recast(totalCount int64, vertStart uint32, schema PropertySchema) {
	//VertexProperty Recast
	// var oldVertexProp VertexProperties
	oldVertexProp := make(map[string]*VValues, len(schema.Schema))
	for _, k := range schema.Schema {
		oldVertexProp[k.PropKey] = &VValues{
			VType:  (*vp)[k.PropKey].VType,
			Values: (*vp)[k.PropKey].Values,
		}
		switch (*vp)[k.PropKey].VType {
		case ValueTypeInt32:
			(*vp)[k.PropKey].Values = make([]serialize.SInt32, totalCount)
			for i, v := range oldVertexProp[k.PropKey].Values.([]serialize.SInt32) {
				(*vp)[k.PropKey].Values.([]serialize.SInt32)[vertStart+uint32(i)] = v
			}
		case ValueTypeFloat32:
			(*vp)[k.PropKey].Values = make([]serialize.SFloat32, totalCount)
			for i, v := range oldVertexProp[k.PropKey].Values.([]serialize.SFloat32) {
				(*vp)[k.PropKey].Values.([]serialize.SFloat32)[vertStart+uint32(i)] = v
			}
		case ValueTypeString:
			(*vp)[k.PropKey].Values = make([]serialize.SString, totalCount)
			for i, v := range oldVertexProp[k.PropKey].Values.([]serialize.SString) {
				(*vp)[k.PropKey].Values.([]serialize.SString)[vertStart+uint32(i)] = v
			}
		}
	}
}

type EdgeProperties map[string]*VValues

func (ep *EdgeProperties) Init(schema PropertySchema, vertexCount uint32) {
	*ep = make(map[string]*VValues, len(schema.Schema))
	for _, v := range schema.Schema {
		(*ep)[v.PropKey] = &VValues{}
		(*ep)[v.PropKey].VType = v.VType
		switch v.VType {
		case ValueTypeInt32:
			(*ep)[v.PropKey].Values = make([][]serialize.SInt32, vertexCount)
		case ValueTypeFloat32:
			(*ep)[v.PropKey].Values = make([][]serialize.SFloat32, vertexCount)
		case ValueTypeString:
			(*ep)[v.PropKey].Values = make([][]serialize.SString, vertexCount)
		}
	}
}
func (ep *EdgeProperties) GetInt32Value(propKey string, vertID, idx uint32) serialize.SInt32 {
	value := (*ep)[propKey].Values.([][]serialize.SInt32)[vertID][idx]
	return value
}

func (ep *EdgeProperties) GetStringValue(propKey string, vertID, idx uint32) serialize.SString {
	value := (*ep)[propKey].Values.([][]serialize.SString)[vertID][idx]
	return value
}

func (ep *EdgeProperties) GetFloat32Value(propKey string, vertID, idx uint32) serialize.SFloat32 {
	value := (*ep)[propKey].Values.([][]serialize.SFloat32)[vertID][idx]
	return value
}

func (ep *EdgeProperties) GetValue(propKey string, vertID, idx uint32) (serialize.MarshalAble, error) {
	var value serialize.MarshalAble
	// if idx < 0 {
	// 	return nil, fmt.Errorf("idx:%v out of range", idx)
	// }
	_, ok := (*ep)[propKey]
	if !ok {
		return nil, fmt.Errorf("property key:%v not exist", propKey)
	}
	switch (*ep)[propKey].VType {
	case ValueTypeInt32:
		if int(idx) >= len((*ep)[propKey].Values.([][]serialize.SInt32)[vertID]) {
			return nil, fmt.Errorf("idx:%v out of range", idx)
		}
		value = &(*ep)[propKey].Values.([][]serialize.SInt32)[vertID][idx]
	case ValueTypeFloat32:
		if int(idx) >= len((*ep)[propKey].Values.([][]serialize.SFloat32)[vertID]) {
			return nil, fmt.Errorf("idx:%v out of range", idx)
		}
		value = &(*ep)[propKey].Values.([][]serialize.SFloat32)[vertID][idx]
	case ValueTypeString:
		if int(idx) >= len((*ep)[propKey].Values.([][]serialize.SString)[vertID]) {
			return nil, fmt.Errorf("idx:%v out of range", idx)
		}
		value = &(*ep)[propKey].Values.([][]serialize.SString)[vertID][idx]
	}
	return value, nil
}

func (ep *EdgeProperties) GetValueType(propKey string) (ValueType, bool) {
	prop, ok := (*ep)[propKey]
	if !ok {
		return ValueTypeUnknow, false
	}
	return prop.VType, ok
}

func (ep *EdgeProperties) AppendProp(prop PropertyValue, inIdx uint32, schema PropertySchema) {
	values := prop.CopyProperty(schema)
	for i, v := range schema.Schema {
		switch v.VType {
		case ValueTypeInt32:
			(*ep)[v.PropKey].Values.([][]serialize.SInt32)[inIdx] =
				append((*ep)[v.PropKey].Values.([][]serialize.SInt32)[inIdx],
					*values[i].(*serialize.SInt32))
		case ValueTypeFloat32:
			(*ep)[v.PropKey].Values.([][]serialize.SFloat32)[inIdx] =
				append((*ep)[v.PropKey].Values.([][]serialize.SFloat32)[inIdx],
					*values[i].(*serialize.SFloat32))
		case ValueTypeString:
			(*ep)[v.PropKey].Values.([][]serialize.SString)[inIdx] =
				append((*ep)[v.PropKey].Values.([][]serialize.SString)[inIdx],
					*values[i].(*serialize.SString))
		}
	}
}

func (ep *EdgeProperties) OptimizeMemory() {
	for k, v := range *ep {
		switch v.VType {
		case ValueTypeInt32:
			for i := range (*ep)[k].Values.([][]serialize.SInt32) {
				values := make([]serialize.SInt32, 0, len((*ep)[k].Values.([][]serialize.SInt32)[i]))
				values = append(values, (*ep)[k].Values.([][]serialize.SInt32)[i]...)
				(*ep)[k].Values.([][]serialize.SInt32)[i] = values
			}
		case ValueTypeFloat32:
			for i := range (*ep)[k].Values.([][]serialize.SFloat32) {
				values := make([]serialize.SFloat32, 0, len((*ep)[k].Values.([][]serialize.SFloat32)[i]))
				values = append(values, (*ep)[k].Values.([][]serialize.SFloat32)[i]...)
				(*ep)[k].Values.([][]serialize.SFloat32)[i] = values
			}
		case ValueTypeString:
			for i := range (*ep)[k].Values.([][]serialize.SString) {
				values := make([]serialize.SString, 0, len((*ep)[k].Values.([][]serialize.SString)[i]))
				values = append(values, (*ep)[k].Values.([][]serialize.SString)[i]...)
				(*ep)[k].Values.([][]serialize.SString)[i] = values
			}
		}
	}
}
