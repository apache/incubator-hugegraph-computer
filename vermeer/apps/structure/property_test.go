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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPropertySchema_Marshal(t *testing.T) {
	type fields struct {
		Schema    []*PSchema
		HgPSchema *HugegraphPSchema
	}
	type args struct {
		buffer []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		// TODO: Add test cases.
		{"empty", fields{[]*PSchema{}, nil}, args{[]byte{}}, 8, false},
		{"empty but valid", fields{[]*PSchema{}, &HugegraphPSchema{}}, args{[]byte{}}, 16, false},
		{"only PSchema", fields{[]*PSchema{
			{VType: 0, PropKey: "a"}, {VType: 1, PropKey: "b"}, {VType: 2, PropKey: "c"},
		}, nil}, args{[]byte{}}, 23, false},
		{"all schema", fields{[]*PSchema{
			{VType: 0, PropKey: "a"}, {VType: 1, PropKey: "b"}, {VType: 2, PropKey: "c"}},
			&HugegraphPSchema{
				PropIndex: map[int]int{1: 10, 2: 20, 3: 30},
				Labels:    map[int]string{1: "v1", 2: "v2", 3: "v3"},
			}}, args{[]byte{}}, 79, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ps := &PropertySchema{
				Schema:    tt.fields.Schema,
				HgPSchema: tt.fields.HgPSchema,
			}
			tt.args.buffer = make([]byte, ps.PredictSize()+16)
			got, err := ps.Marshal(tt.args.buffer)
			if (err != nil) != tt.wantErr {
				t.Errorf("Marshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Marshal() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPropertySchema_Unmarshal(t *testing.T) {
	type fields struct {
		Schema    []*PSchema
		HgPSchema *HugegraphPSchema
	}
	type args struct {
		buffer []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		// TODO: Add test cases.
		{"empty", fields{[]*PSchema{}, nil}, args{[]byte{}}, 8, false},
		{"empty but valid", fields{[]*PSchema{}, &HugegraphPSchema{map[int]int{}, map[int]string{}}}, args{[]byte{}}, 16, false},
		{"only PSchema", fields{[]*PSchema{
			{VType: 0, PropKey: "a"}, {VType: 1, PropKey: "b"}, {VType: 2, PropKey: "c"},
		}, nil}, args{[]byte{}}, 23, false},
		{"all schema", fields{[]*PSchema{
			{VType: 0, PropKey: "a"}, {VType: 1, PropKey: "b"}, {VType: 2, PropKey: "c"}},
			&HugegraphPSchema{
				PropIndex: map[int]int{1: 10, 2: 20, 3: 30},
				Labels:    map[int]string{1: "v1", 2: "v2", 3: "v3"},
			}}, args{[]byte{}}, 79, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ps := &PropertySchema{
				Schema:    tt.fields.Schema,
				HgPSchema: tt.fields.HgPSchema,
			}
			tt.args.buffer = make([]byte, ps.PredictSize()+16)
			got, err := ps.Marshal(tt.args.buffer)
			if (err != nil) != tt.wantErr {
				t.Errorf("Marshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Marshal() got = %v, want %v", got, tt.want)
			}
			ps = &PropertySchema{}
			got, err = ps.Unmarshal(tt.args.buffer)
			//if (err != nil) != tt.wantErr {
			//	t.Errorf("Unmarshal() error = %v, wantErr %v", err, tt.wantErr)
			//	return
			//}
			//if got != tt.want {
			//	t.Errorf("Unmarshal() got = %v, want %v", got, tt.want)
			//}
			for i, schema := range ps.Schema {
				assert.Equal(t, &tt.fields.Schema[i], &schema)
			}
			if tt.fields.HgPSchema != nil {
				assert.Equal(t, tt.fields.HgPSchema.PropIndex, ps.HgPSchema.PropIndex)
				assert.Equal(t, tt.fields.HgPSchema.Labels, ps.HgPSchema.Labels)
			}
		})
	}
}
