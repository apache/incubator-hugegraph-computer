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

// Deserialization of user define type

func (br *BufferReader) ReadStruct(st Serializable) error {
	return st.Load(br)
}

// serialize of user define type

func (bw *BufferWriter) WriteStruct(st Serializable) error {
	return st.Save(bw)
}

//func (br *BufferReader) ReadSStruct(sts []Serializable) error {
//	l, err := br.ReadUInt32()
//	if err != nil {
//		return err
//	}
//
//	for _, st := range sts {
//		err = br.ReadStruct(st)
//		if err != nil {
//			return err
//		}
//	}
//
//	return nil
//}
//
//func (bw *BufferWriter) WriteSStruct(sts []Serializable) error {
//	err := bw.WriteUInt32(uint32(len(sts)))
//	if err != nil {
//		return err
//	}
//
//	for _, st := range sts {
//		err = bw.WriteStruct(st)
//		if err != nil {
//			return err
//		}
//	}
//	return nil
//}
