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

package buffer

import (
	"vermeer/apps/serialize"

	"github.com/sirupsen/logrus"
)

type EncodeBuffer struct {
	buffer    []byte
	offset    int
	items     int
	threshold int
}

func (eb *EncodeBuffer) Init(size int) {
	eb.buffer = make([]byte, size)
	eb.offset = 0
	eb.threshold = size - size/10
	eb.items = 0
}

func (eb *EncodeBuffer) FromBytes(data []byte) {
	eb.buffer = data
	eb.offset = len(data)
	eb.threshold = len(data)
}

func (eb *EncodeBuffer) grow(n int) {
	logrus.Infof("buffer grow %d -> %d", len(eb.buffer), n+len(eb.buffer))
	newBuffer := make([]byte, n+len(eb.buffer))
	copy(newBuffer, eb.buffer)
	eb.buffer = newBuffer
}

func (eb *EncodeBuffer) Marshal(obj serialize.MarshalAble) error {
	if obj.PredictSize() > len(eb.buffer)-eb.offset-1024 {
		eb.grow(obj.PredictSize() + 1024)
	}
	n, err := obj.Marshal(eb.buffer[eb.offset:])
	if err != nil {
		return err
	}
	eb.offset += n
	eb.items += 1
	return err
}

func (eb *EncodeBuffer) Unmarshal(obj serialize.MarshalAble) error {
	n, err := obj.Unmarshal(eb.buffer[eb.offset:])
	if err != nil {
		return err
	}
	eb.offset += n
	eb.items += 1
	return err
}

func (eb *EncodeBuffer) PayLoad() []byte {
	return eb.buffer[:eb.offset]
}

func (eb *EncodeBuffer) Full() bool {
	return eb.offset >= eb.threshold
}

func (eb *EncodeBuffer) Reset() {
	eb.offset = 0
	eb.items = 0
}

func (eb *EncodeBuffer) ObjCount() int {
	return eb.items
}
