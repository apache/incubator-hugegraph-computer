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
	"bufio"
	"io"
)

const defaultCapacity = 4 * 1024 * 1024

// BufferReader 反序列化
type BufferReader struct {
	*bufio.Reader
}

func NewReader(rd io.Reader) *BufferReader {
	return NewReaderSize(rd, defaultCapacity)
}

func NewReaderSize(rd io.Reader, capacity int) *BufferReader {
	br := BufferReader{
		Reader: bufio.NewReaderSize(rd, capacity),
	}
	return &br
}

// BufferWriter 序列化
type BufferWriter struct {
	*bufio.Writer
}

func NewWriter(wt io.Writer) *BufferWriter {
	return NewWriterSize(wt, defaultCapacity)
}

func NewWriterSize(wt io.Writer, capacity int) *BufferWriter {
	bw := BufferWriter{
		Writer: bufio.NewWriterSize(wt, capacity),
	}

	return &bw
}

// Serializable 用户自定义结构体需要实现此接口以支持序列化及反序列化
type Serializable interface {
	Save(bw *BufferWriter) error
	Load(br *BufferReader) error
}
