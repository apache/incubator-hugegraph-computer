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

type EncodeBuffer struct {
	Buffer    []byte
	offset    int
	items     int
	threshold int
}

func (eb *EncodeBuffer) Init(size int) {
	eb.Buffer = make([]byte, size)
	eb.offset = 0
	eb.threshold = size / 4 * 3
	eb.items = 0
}

func (eb *EncodeBuffer) FromBytes(data []byte) {
	eb.Buffer = data
	eb.offset = len(data)
	eb.threshold = len(data)
}

func (eb *EncodeBuffer) Marshal(obj MarshalAble) error {
	n, err := obj.Marshal(eb.Buffer[eb.offset:])
	if err != nil {
		return err
	}
	eb.offset += n
	eb.items += 1
	return err
}

func (eb *EncodeBuffer) Unmarshal(obj MarshalAble) error {
	n, err := obj.Unmarshal(eb.Buffer[eb.offset:])
	if err != nil {
		return err
	}
	eb.offset += n
	eb.items += 1
	return err
}

func (eb *EncodeBuffer) PayLoad() []byte {
	return eb.Buffer[:eb.offset]
}

func (eb *EncodeBuffer) Full() bool {
	return eb.offset >= eb.threshold
}

func (eb *EncodeBuffer) Grow(n int) {
	if n > len(eb.Buffer) {
		newBuffer := make([]byte, n)
		copy(newBuffer, eb.Buffer)
		eb.Buffer = newBuffer
	}
}

func (eb *EncodeBuffer) Reset() {
	eb.offset = 0
	eb.items = 0
}

func (eb *EncodeBuffer) ObjCount() int {
	return eb.items
}
