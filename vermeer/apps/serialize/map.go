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
	"time"

	"github.com/bytedance/sonic"
	"github.com/sirupsen/logrus"
)

type MapUint32Uint8 map[SUint32]SUint8

func (mui *MapUint32Uint8) Marshal(buffer []byte) (int, error) {
	offset := 0
	binary.BigEndian.PutUint32(buffer, uint32(len(*mui)))
	offset += 4
	for k, v := range *mui {
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

func (mui *MapUint32Uint8) Unmarshal(buffer []byte) (int, error) {
	offset := 0
	length := binary.BigEndian.Uint32(buffer)
	offset += 4
	*mui = make(map[SUint32]SUint8, length)
	for i := 0; i < int(length); i++ {
		var k SUint32
		n, err := k.Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
		var v SUint8
		n, err = v.Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
		(*mui)[k] = v
	}
	return offset, nil
}

func (mui *MapUint32Uint8) ToString() string {
	return ""
}

func (mui *MapUint32Uint8) PredictSize() int {
	return 4 + 5*len(*mui)
}

type MapUint32Uint32 map[SUint32]SUint32

func (mui32 *MapUint32Uint32) Marshal(buffer []byte) (int, error) {
	offset := 0
	binary.BigEndian.PutUint32(buffer, uint32(len(*mui32)))
	offset += 4
	for k, v := range *mui32 {
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

func (mui32 *MapUint32Uint32) Unmarshal(buffer []byte) (int, error) {
	offset := 0
	length := binary.BigEndian.Uint32(buffer)
	offset += 4
	*mui32 = make(map[SUint32]SUint32, length)
	for i := 0; i < int(length); i++ {
		var k SUint32
		n, err := k.Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
		var v SUint32
		n, err = v.Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
		(*mui32)[k] = v
	}
	return offset, nil
}

func (mui32 *MapUint32Uint32) ToString() string {
	return ""
}

func (mui32 *MapUint32Uint32) PredictSize() int {
	return 4 + 8*len(*mui32)
}

type MapUint32Float32 map[SUint32]SFloat32

func (muf *MapUint32Float32) Marshal(buffer []byte) (int, error) {
	offset := 0
	binary.BigEndian.PutUint32(buffer, uint32(len(*muf)))
	offset += 4
	for k, v := range *muf {
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

func (muf *MapUint32Float32) Unmarshal(buffer []byte) (int, error) {
	offset := 0
	length := binary.BigEndian.Uint32(buffer)
	offset += 4
	*muf = make(map[SUint32]SFloat32, length)
	for i := 0; i < int(length); i++ {
		var k SUint32
		n, err := k.Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
		var v SFloat32
		n, err = v.Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
		(*muf)[k] = v
	}
	return offset, nil
}

func (muf *MapUint32Float32) ToString() string {
	bytes, err := sonic.Marshal(*muf)
	if err != nil {
		logrus.Errorf("MapUint32Float32.ToString() marshal error: %v", err)
		return ""
	}
	return string(bytes)
}

func (muf *MapUint32Float32) PredictSize() int {
	return 4 + 8*len(*muf)
}

type MapUint32Struct map[SUint32]struct{}

func (mus *MapUint32Struct) Marshal(buffer []byte) (int, error) {
	offset := 0
	binary.BigEndian.PutUint32(buffer, uint32(len(*mus)))
	offset += 4
	for k := range *mus {
		n, err := k.Marshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	return offset, nil
}

func (mus *MapUint32Struct) Unmarshal(buffer []byte) (int, error) {
	offset := 0
	length := binary.BigEndian.Uint32(buffer)
	offset += 4
	*mus = make(map[SUint32]struct{}, length)
	for i := 0; i < int(length); i++ {
		var k SUint32
		n, err := k.Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
		(*mus)[k] = struct{}{}
	}
	return offset, nil
}

func (mus *MapUint32Struct) ToString() string {
	return ""
}

func (mus *MapUint32Struct) PredictSize() int {
	return 4 + 4*len(*mus)
}

type MapStringStruct map[SString]struct{}

func (mus *MapStringStruct) Marshal(buffer []byte) (int, error) {
	offset := 0
	binary.BigEndian.PutUint32(buffer, uint32(len(*mus)))
	offset += 4
	for k := range *mus {
		n, err := k.Marshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	return offset, nil
}

func (mus *MapStringStruct) Unmarshal(buffer []byte) (int, error) {
	offset := 0
	length := binary.BigEndian.Uint32(buffer)
	offset += 4
	*mus = make(map[SString]struct{}, length)
	for i := 0; i < int(length); i++ {
		var k SString
		n, err := k.Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
		(*mus)[k] = struct{}{}
	}
	return offset, nil
}

func (mus *MapStringStruct) ToString() string {
	return ""
}

func (mus *MapStringStruct) PredictSize() int {
	count := 4
	for sString := range *mus {
		count += sString.PredictSize()
	}
	return count
}

type MapStringUint32 map[string]uint32

func (msu *MapStringUint32) Marshal(buffer []byte) (int, error) {
	offset := 0
	binary.BigEndian.PutUint32(buffer, uint32(len(*msu)))
	offset += 4
	for k, v := range *msu {
		kk := SString(k)
		n, err := kk.Marshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
		vv := SUint32(v)
		n, err = vv.Marshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
	}
	return offset, nil
}

func (msu *MapStringUint32) Unmarshal(buffer []byte) (int, error) {
	offset := 0
	length := binary.BigEndian.Uint32(buffer)
	offset += 4
	*msu = make(map[string]uint32, length)
	for i := 0; i < int(length); i++ {
		var k SString
		n, err := k.Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
		var v SUint32
		n, err = v.Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
		(*msu)[string(k)] = uint32(v)
	}
	return offset, nil
}

func (msu *MapStringUint32) ToString() string {
	return ""
}

func (msu *MapStringUint32) PredictSize() int {
	size := 4
	for k, v := range *msu {
		kk := SString(k)
		size += kk.PredictSize()

		vv := SUint32(v)
		size += vv.PredictSize()
	}
	return size
}

func (msu *MapStringUint32) Partition(limit int) []*MapStringUint32 {

	p := make([]*MapStringUint32, 0)
	tt := time.Now()
	size := msu.PredictSize()
	logrus.Infof("Serialize  MapStringUint32 PredictSize  cost=%v\n", time.Since(tt))

	var partNum int
	if size/limit == 0 {
		partNum = 1
	} else {
		partNum = size / limit
	}
	for i := 0; i < partNum; i++ {
		one := make(MapStringUint32)
		p = append(p, &one)
	}

	c := 0
	for k, v := range *msu {
		index := c % partNum
		(*p[index])[k] = v
		c += 1
	}
	return p
}

type MapUint32MapStrUint32 map[SUint32]MapStringUint32

func (mui32 *MapUint32MapStrUint32) Marshal(buffer []byte) (int, error) {
	offset := 0
	binary.BigEndian.PutUint32(buffer, uint32(len(*mui32)))
	offset += 4
	for k, v := range *mui32 {
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

func (mui32 *MapUint32MapStrUint32) Unmarshal(buffer []byte) (int, error) {
	offset := 0
	length := binary.BigEndian.Uint32(buffer)
	offset += 4
	*mui32 = make(map[SUint32]MapStringUint32, length)
	for i := 0; i < int(length); i++ {
		var k SUint32
		n, err := k.Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
		var v MapStringUint32
		n, err = v.Unmarshal(buffer[offset:])
		if err != nil {
			return 0, err
		}
		offset += n
		(*mui32)[k] = v
	}
	return offset, nil
}

func (mui32 *MapUint32MapStrUint32) ToString() string {
	return ""
}

func (mui32 *MapUint32MapStrUint32) PredictSize() int {
	length := 4
	for k, v := range *mui32 {
		length += k.PredictSize()
		length += v.PredictSize()
	}
	return length
}
