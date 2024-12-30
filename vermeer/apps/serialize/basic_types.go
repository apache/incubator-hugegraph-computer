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
	"math"
	"strconv"
	"unsafe"
)

type MarshalAble interface {
	Marshal(buffer []byte) (int, error)
	Unmarshal(buffer []byte) (int, error)
	ToString() string
	PredictSize() int
}

type SUint8 uint8
type SUint32 uint32
type SUint64 uint64
type SFloat32 float32
type SFloat64 float64
type SInt32 int32
type SInt64 int64
type SString string

func (si *SUint8) Marshal(buffer []byte) (int, error) {
	buffer[0] = byte(*si)
	return 1, nil
}

func (si *SUint8) Unmarshal(buffer []byte) (int, error) {
	*si = SUint8(buffer[0])
	return 1, nil
}

func (si *SUint8) ToString() string {
	return strconv.FormatInt(int64(*si), 10)
}

func (si *SUint8) PredictSize() int {
	return 1
}

func (si *SUint32) Marshal(buffer []byte) (int, error) {
	binary.BigEndian.PutUint32(buffer, uint32(*si))
	return 4, nil
}

func (si *SUint32) Unmarshal(buffer []byte) (int, error) {
	*si = SUint32(binary.BigEndian.Uint32(buffer))
	return 4, nil
}

func (si *SUint32) ToString() string {
	return strconv.FormatInt(int64(*si), 10)
}

func (si *SUint32) PredictSize() int {
	return 4
}

func (si *SUint64) Marshal(buffer []byte) (int, error) {
	binary.BigEndian.PutUint64(buffer, uint64(*si))
	return 8, nil
}

func (si *SUint64) Unmarshal(buffer []byte) (int, error) {
	*si = SUint64(binary.BigEndian.Uint64(buffer))
	return 8, nil
}

func (si *SUint64) ToString() string {
	return strconv.FormatInt(int64(*si), 10)
}

func (si *SUint64) PredictSize() int {
	return 8
}

func (sf *SFloat32) Marshal(buffer []byte) (int, error) {
	binary.BigEndian.PutUint32(buffer, math.Float32bits(float32(*sf)))
	return 4, nil
}

func (sf *SFloat32) Unmarshal(buffer []byte) (int, error) {
	*sf = SFloat32(math.Float32frombits(binary.BigEndian.Uint32(buffer)))
	return 4, nil
}

func (sf *SFloat32) ToString() string {
	return strconv.FormatFloat(float64(*sf), 'E', -1, 32)
}

func (sf *SFloat32) PredictSize() int {
	return 4
}

func (sf *SFloat64) Marshal(buffer []byte) (int, error) {
	binary.BigEndian.PutUint64(buffer, math.Float64bits(float64(*sf)))
	return 8, nil
}

func (sf *SFloat64) Unmarshal(buffer []byte) (int, error) {
	*sf = SFloat64(math.Float64frombits(binary.BigEndian.Uint64(buffer)))
	return 8, nil
}

func (sf *SFloat64) ToString() string {
	return strconv.FormatFloat(float64(*sf), 'E', -1, 64)
}

func (sf *SFloat64) PredictSize() int {
	return 8
}

func (si *SInt32) Marshal(buffer []byte) (int, error) {
	binary.BigEndian.PutUint32(buffer, uint32(*si))
	return 4, nil
}

func (si *SInt32) Unmarshal(buffer []byte) (int, error) {
	*si = SInt32(binary.BigEndian.Uint32(buffer))
	return 4, nil
}

func (si *SInt32) ToString() string {
	return strconv.Itoa(int(*si))
}

func (si *SInt32) PredictSize() int {
	return 4
}

func (si *SInt64) Marshal(buffer []byte) (int, error) {
	binary.BigEndian.PutUint64(buffer, uint64(*si))
	return 8, nil
}

func (si *SInt64) Unmarshal(buffer []byte) (int, error) {
	*si = SInt64(binary.BigEndian.Uint64(buffer))
	return 8, nil
}

func (si *SInt64) ToString() string {
	return strconv.FormatInt(int64(*si), 10)
}

func (si *SInt64) PredictSize() int {
	return 8
}

func (ss *SString) Marshal(buffer []byte) (int, error) {
	offset := 0
	binary.BigEndian.PutUint16(buffer, uint16(len(*ss)))
	offset += 2
	copy(buffer[offset:], *ss)
	offset += len(*ss)
	return offset, nil
}

func (ss *SString) Unmarshal(buffer []byte) (int, error) {
	offset := 0
	sLen := binary.BigEndian.Uint16(buffer)
	offset += 2
	b := make([]byte, sLen)
	copy(b, buffer[offset:])
	*ss = *(*SString)(unsafe.Pointer(&b))
	offset += int(sLen)
	return offset, nil
}

func (ss *SString) ToString() string {
	return string(*ss)
}

func (ss *SString) PredictSize() int {
	return len(*ss) + 2
}
