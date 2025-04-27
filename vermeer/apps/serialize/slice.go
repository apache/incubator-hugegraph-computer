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

package serialize

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

// Deserialization of slice

type SliceInt32 []SInt32
type SliceInt64 []SInt64
type SliceFloat32 []SFloat32
type SliceUint32 []SUint32
type TwoDimSliceUint32 []SliceUint32

func (su *SliceUint32) Marshal(buffer []byte) (int, error) {
	offset := 0
	binary.BigEndian.PutUint32(buffer, uint32(len(*su)))
	offset += 4
	for _, v := range *su {
		n, err := v.Marshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	return offset, nil
}

func (su *SliceUint32) Unmarshal(buffer []byte) (int, error) {
	offset := 0
	length := binary.BigEndian.Uint32(buffer)
	offset += 4
	*su = make([]SUint32, length)
	for i := uint32(0); i < length; i++ {
		n, err := (*su)[i].Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	return offset, nil
}

func (su *SliceUint32) ToString() string {
	return fmt.Sprintf("%v", *su)
}

func (su *SliceUint32) PredictSize() int {
	return 4 + 4*len(*su)
}

func (su *SliceUint32) Partition(limit int) []SlicePartition {

	p := make([]SlicePartition, 0)

	length := len(*su)

	index := 0
	size := 4
	for i, v := range *su {
		size += v.PredictSize()
		if size >= limit {
			var one SlicePartition
			one.Start = index
			one.Size = size
			one.End = i + 1
			p = append(p, one)

			index = i + 1
			size = 4
		}
		if i == length-1 && index < length {
			var lastOne SlicePartition
			lastOne.Size = size
			lastOne.Start = index
			lastOne.End = length
			p = append(p, lastOne)

		}

	}
	return p
}

func (tsu *TwoDimSliceUint32) Marshal(buffer []byte) (int, error) {
	offset := 0
	binary.BigEndian.PutUint32(buffer, uint32(len(*tsu)))
	offset += 4
	for _, v := range *tsu {
		n, err := v.Marshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	return offset, nil
}

func (tsu *TwoDimSliceUint32) Unmarshal(buffer []byte) (int, error) {
	offset := 0
	length := binary.BigEndian.Uint32(buffer)
	offset += 4
	*tsu = make([]SliceUint32, length)
	for i := range *tsu {
		var v SliceUint32
		n, err := v.Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		(*tsu)[i] = v
		offset += n
	}
	return offset, nil
}

func (tsu *TwoDimSliceUint32) ToString() string {
	return fmt.Sprintf("%v", *tsu)
}

func (tsu *TwoDimSliceUint32) PredictSize() int {
	size := 4
	for i := range *tsu {
		size += 4 + 4*len((*tsu)[i])
	}
	return size
}

func (tsu *TwoDimSliceUint32) Partition(limit int) []SlicePartition {

	p := make([]SlicePartition, 0)

	length := len(*tsu)

	index := 0
	size := 4
	for i, v := range *tsu {
		size += v.PredictSize()
		if size >= limit {
			var one SlicePartition
			one.Start = index
			one.Size = size
			one.End = i + 1
			p = append(p, one)

			index = i + 1
			size = 4
		}
		if i == length-1 && index < length {
			var lastOne SlicePartition
			lastOne.Size = size
			lastOne.Start = index
			lastOne.End = length
			p = append(p, lastOne)

		}

	}
	return p
}

func (tsu *TwoDimSliceUint32) MarshalJSON() ([]byte, error) {

	value := []byte("value")
	return json.Marshal(string(value))
}

func (tsu *TwoDimSliceUint32) UnmarshalJSON(buffer []byte) error {

	v := ""
	return json.Unmarshal(buffer, &v)

}

func (si *SliceInt32) Marshal(buffer []byte) (int, error) {
	offset := 0
	binary.BigEndian.PutUint32(buffer, uint32(len(*si)))
	offset += 4
	for _, v := range *si {
		n, err := v.Marshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	return offset, nil
}

func (si *SliceInt32) Unmarshal(buffer []byte) (int, error) {
	offset := 0
	length := binary.BigEndian.Uint32(buffer)
	offset += 4
	*si = make([]SInt32, length)
	for i := uint32(0); i < length; i++ {
		n, err := (*si)[i].Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	return offset, nil
}

func (si *SliceInt32) ToString() string {
	return ""
}

func (si *SliceInt32) PredictSize() int {
	return 4 + 4*len(*si)
}

func (si *SliceInt32) Partition(limit int) []SlicePartition {

	p := make([]SlicePartition, 0)

	length := len(*si)

	index := 0
	size := 4
	for i, v := range *si {
		size += v.PredictSize()
		if size >= limit {
			var one SlicePartition
			one.Start = index
			one.Size = size
			one.End = i + 1
			p = append(p, one)

			index = i + 1
			size = 4
		}
		if i == length-1 && index < length {
			var lastOne SlicePartition
			lastOne.Size = size
			lastOne.Start = index
			lastOne.End = length
			p = append(p, lastOne)

		}

	}
	return p

}

type TwoDimSliceInt32 [][]SInt32

func (tsi *TwoDimSliceInt32) Marshal(buffer []byte) (int, error) {
	offset := 0
	binary.BigEndian.PutUint32(buffer, uint32(len(*tsi)))
	offset += 4
	for _, v := range *tsi {
		vv := SliceInt32(v)
		n, err := vv.Marshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	return offset, nil
}

func (tsi *TwoDimSliceInt32) Unmarshal(buffer []byte) (int, error) {
	offset := 0
	length := binary.BigEndian.Uint32(buffer)
	offset += 4
	*tsi = make([][]SInt32, length)
	for i := range *tsi {
		var v SliceInt32
		n, err := v.Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		(*tsi)[i] = v
		offset += n
	}
	return offset, nil
}

func (tsi *TwoDimSliceInt32) ToString() string {
	return ""
}

func (tsi *TwoDimSliceInt32) PredictSize() int {
	size := 4
	for i := range *tsi {
		size += 4 + 4*len((*tsi)[i])
	}
	return size
}

func (tsi *TwoDimSliceInt32) Partition(limit int) []SlicePartition {

	p := make([]SlicePartition, 0)

	length := len(*tsi)

	index := 0
	size := 4
	for i, v := range *tsi {
		vv := SliceInt32(v)
		size += vv.PredictSize()
		if size >= limit {
			var one SlicePartition
			one.Start = index
			one.Size = size
			one.End = i + 1
			p = append(p, one)

			index = i + 1
			size = 4
		}
		if i == length-1 && index < length {
			var lastOne SlicePartition
			lastOne.Size = size
			lastOne.Start = index
			lastOne.End = length
			p = append(p, lastOne)

		}

	}
	return p

}

func (sf *SliceFloat32) Marshal(buffer []byte) (int, error) {
	offset := 0
	binary.BigEndian.PutUint32(buffer, uint32(len(*sf)))
	offset += 4
	for _, v := range *sf {
		n, err := v.Marshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	return offset, nil
}

func (sf *SliceFloat32) Unmarshal(buffer []byte) (int, error) {
	offset := 0
	length := binary.BigEndian.Uint32(buffer)
	offset += 4
	*sf = make([]SFloat32, length)
	for i := uint32(0); i < length; i++ {
		n, err := (*sf)[i].Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	return offset, nil
}

func (sf *SliceFloat32) ToString() string {
	return ""
}

func (sf *SliceFloat32) PredictSize() int {
	return 4 + 4*len(*sf)
}

func (sf *SliceFloat32) Partition(limit int) []SlicePartition {

	p := make([]SlicePartition, 0)

	length := len(*sf)

	index := 0
	size := 4
	for i, v := range *sf {
		size += v.PredictSize()
		if size >= limit {
			var one SlicePartition
			one.Start = index
			one.Size = size
			one.End = i + 1
			p = append(p, one)

			index = i + 1
			size = 4
		}
		if i == length-1 && index < length {
			var lastOne SlicePartition
			lastOne.Size = size
			lastOne.Start = index
			lastOne.End = length
			p = append(p, lastOne)

		}

	}
	return p
}

type TwoDimSliceFloat32 [][]SFloat32

func (tsf *TwoDimSliceFloat32) Marshal(buffer []byte) (int, error) {
	offset := 0
	binary.BigEndian.PutUint32(buffer, uint32(len(*tsf)))
	offset += 4
	for _, v := range *tsf {
		vv := SliceFloat32(v)
		n, err := vv.Marshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	return offset, nil
}

func (tsf *TwoDimSliceFloat32) Unmarshal(buffer []byte) (int, error) {
	offset := 0
	length := binary.BigEndian.Uint32(buffer)
	offset += 4
	*tsf = make([][]SFloat32, length)
	for i := range *tsf {
		var v SliceFloat32
		n, err := v.Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		(*tsf)[i] = v
		offset += n
	}
	return offset, nil
}

func (tsf *TwoDimSliceFloat32) ToString() string {
	return ""
}

func (tsf *TwoDimSliceFloat32) PredictSize() int {
	size := 4
	for i := range *tsf {
		size += 4 + 4*len((*tsf)[i])
	}
	return size
}

func (tsf *TwoDimSliceFloat32) Partition(limit int) []SlicePartition {

	p := make([]SlicePartition, 0)

	length := len(*tsf)

	index := 0
	size := 4
	for i, v := range *tsf {
		vv := SliceFloat32(v)
		size += vv.PredictSize()
		if size >= limit {
			var one SlicePartition
			one.Start = index
			one.Size = size
			one.End = i + 1
			p = append(p, one)

			index = i + 1
			size = 4
		}
		if i == length-1 && index < length {
			var lastOne SlicePartition
			lastOne.Size = size
			lastOne.Start = index
			lastOne.End = length
			p = append(p, lastOne)

		}

	}
	return p
}

type SliceString []SString

func (ss *SliceString) Marshal(buffer []byte) (int, error) {
	offset := 0
	binary.BigEndian.PutUint32(buffer, uint32(len(*ss)))
	offset += 4
	for _, v := range *ss {
		n, err := v.Marshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	return offset, nil
}

func (ss *SliceString) Unmarshal(buffer []byte) (int, error) {
	offset := 0
	length := binary.BigEndian.Uint32(buffer)
	offset += 4
	*ss = make([]SString, length)
	for i := uint32(0); i < length; i++ {
		n, err := (*ss)[i].Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	return offset, nil
}

func (ss *SliceString) ToString() string {
	return ""
}

func (ss *SliceString) PredictSize() int {

	size := 4
	for _, v := range *ss {
		size += v.PredictSize()
	}
	return size
}

func (ss *SliceString) Partition(limit int) []SlicePartition {
	p := make([]SlicePartition, 0)

	length := len(*ss)

	index := 0
	size := 4
	for i, v := range *ss {
		size += v.PredictSize()
		if size >= limit {
			var one SlicePartition
			one.Start = index
			one.Size = size
			one.End = i + 1
			p = append(p, one)

			index = i + 1
			size = 4
		}
		if i == length-1 && index < length {
			var lastOne SlicePartition
			lastOne.Size = size
			lastOne.Start = index
			lastOne.End = length
			p = append(p, lastOne)

		}

	}
	return p

}

type TwoDimSliceString [][]SString

func (tss *TwoDimSliceString) Marshal(buffer []byte) (int, error) {
	offset := 0
	binary.BigEndian.PutUint32(buffer, uint32(len(*tss)))
	offset += 4
	for _, v := range *tss {
		vv := SliceString(v)
		n, err := vv.Marshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	return offset, nil
}

func (tss *TwoDimSliceString) Unmarshal(buffer []byte) (int, error) {
	offset := 0
	length := binary.BigEndian.Uint32(buffer)
	offset += 4
	*tss = make([][]SString, length)
	for i := range *tss {
		var v SliceString
		n, err := v.Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		(*tss)[i] = v
		offset += n
	}
	return offset, nil
}

func (tss *TwoDimSliceString) ToString() string {
	return fmt.Sprintf("%v", *tss)
}

func (tss *TwoDimSliceString) PredictSize() int {
	size := 4
	for _, v := range *tss {
		vv := SliceString(v)
		size += vv.PredictSize()
	}
	return size
}

func (tss *TwoDimSliceString) Partition(limit int) []SlicePartition {
	p := make([]SlicePartition, 0)

	length := len(*tss)

	index := 0
	size := 4
	for i, v := range *tss {
		vv := SliceString(v)
		size += vv.PredictSize()
		if size >= limit {
			var one SlicePartition
			one.Start = index
			one.Size = size
			one.End = i + 1
			p = append(p, one)

			index = i + 1
			size = 4
		}
		if i == length-1 && index < length {
			var lastOne SlicePartition
			lastOne.Size = size
			lastOne.Start = index
			lastOne.End = length
			p = append(p, lastOne)

		}

	}
	return p

}
func (si *SliceInt64) Marshal(buffer []byte) (int, error) {
	offset := 0
	binary.BigEndian.PutUint32(buffer, uint32(len(*si)))
	offset += 4
	for _, v := range *si {
		n, err := v.Marshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	return offset, nil
}

func (si *SliceInt64) Unmarshal(buffer []byte) (int, error) {
	offset := 0
	length := binary.BigEndian.Uint32(buffer)
	offset += 4
	*si = make([]SInt64, length)
	for i := uint32(0); i < length; i++ {
		n, err := (*si)[i].Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	return offset, nil
}

func (si *SliceInt64) ToString() string {
	return ""
}

func (si *SliceInt64) PredictSize() int {
	return 4 + 8*len(*si)
}

type SlicePartition struct {
	Start int
	End   int
	Size  int
}

type PartsMeta struct {
	FilePrefix string `json:"file_prefix,omitempty"`
	PartNum    int    `json:"part_num,omitempty"`
	ValueType  uint16 `json:"value_type,omitempty"`
}

type KVpair struct {
	K string
	V uint32
}

type SliceKVpair []KVpair

func (sp *SliceKVpair) ToString() string {
	return ""
}

func (sp *SliceKVpair) Marshal(buffer []byte) (int, error) {
	offset := 0
	binary.BigEndian.PutUint32(buffer[offset:], uint32(len(*sp)))
	offset += 4
	for _, pair := range *sp {
		k := SString(pair.K)
		v := SUint32(pair.V)

		n, err := k.Marshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
		n, err = v.Marshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	return offset, nil

}

func (sp *SliceKVpair) Unmarshal(buffer []byte) (int, error) {
	offset := 0
	length := binary.BigEndian.Uint32(buffer[offset:])
	offset += 4

	*sp = make([]KVpair, length)
	for i := range *sp {
		var k SString
		n, err := k.Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
		var v SUint32
		n, err = v.Unmarshal(buffer[offset:])
		offset += n
		pair := KVpair{
			K: string(k),
			V: uint32(v),
		}
		(*sp)[i] = pair
	}
	return offset, nil

}

func (sp *SliceKVpair) Partition(limit int) []SlicePartition {

	p := make([]SlicePartition, 0)

	length := len(*sp)

	index := 0
	size := 4
	for i, v := range *sp {
		size += 4 + 2 + len(v.K)
		if size >= limit {
			var one SlicePartition
			one.Start = index
			one.Size = size
			one.End = i + 1
			p = append(p, one)

			index = i + 1
			size = 4
		}
		if i == length-1 && index < length {
			var lastOne SlicePartition
			lastOne.Size = size
			lastOne.Start = index
			lastOne.End = length
			p = append(p, lastOne)
		}
	}
	return p

}

func (sp *SliceKVpair) PredictSize() int {
	size := 4
	for _, pair := range *sp {
		size += 4 + len(pair.K)
	}
	return size
}
