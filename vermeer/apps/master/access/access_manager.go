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

package access

import (
	"container/list"
	"time"
)

var AccessManager = &accessManager{}

type accessManager struct {
	records *list.List
	m       map[string]*list.Element
}

// TODO: user name?
type AccessRecord struct {
	SpaceName  string
	Name       string
	AccessTime time.Time
}

func (am *accessManager) Init() {
	am.records = list.New()
	am.m = make(map[string]*list.Element)
}

func (am *accessManager) Access(spaceName string, name string) {
	record := new(AccessRecord)
	record.SpaceName = spaceName
	record.Name = name
	record.AccessTime = time.Now()

	if e, ok := am.m[name]; ok {
		am.records.Remove(e)
	}
	e := am.records.PushFront(record)
	am.m[name] = e
}

func (am *accessManager) Recent() *AccessRecord {
	e := am.records.Front()
	if e != nil {
		return e.Value.(*AccessRecord)
	}
	return nil
}

func (am *accessManager) LeastRecent() *AccessRecord {
	e := am.records.Back()
	if e != nil {
		return e.Value.(*AccessRecord)
	}
	return nil
}

func (am *accessManager) LeastRecentRecords() []*AccessRecord {

	records := make([]*AccessRecord, 0)
	for e := am.records.Back(); e != nil; {
		records = append(records, e.Value.(*AccessRecord))
		prev := e.Prev()
		e = prev
	}
	return records
}
