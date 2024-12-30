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

package common

import (
	"sync"
)

// QueueNode 代表每一个节点
type QueueNode struct {
	Data interface{}
	Next *QueueNode
}

type Queue struct {
	// 头节点
	Head *QueueNode

	// 队尾节点
	Tail *QueueNode

	Size int

	locker sync.Mutex
}

func NewQueue() *Queue {
	q := new(Queue)
	q.Head = nil
	q.Tail = nil
	q.Size = 0
	q.locker = sync.Mutex{}
	return q
}

// Put 尾插法
func (q *Queue) Put(element interface{}) {
	q.locker.Lock()
	defer q.locker.Unlock()

	n := new(QueueNode)
	n.Data = element

	if q.Tail == nil {
		q.Head = n
		q.Tail = n
	} else {
		q.Tail.Next = n
		q.Tail = n
	}
	q.Size++
}

// PutHead 头插法，在队列头部插入一个元素
func (q *Queue) PutHead(element interface{}) {
	q.locker.Lock()
	defer q.locker.Unlock()

	n := new(QueueNode)
	n.Data = element

	if q.Head == nil {
		q.Head = n
		q.Tail = n
	} else {
		n.Next = q.Head
		q.Head = n
	}
	q.Size++
}

// Poll 获取并删除队列头部的元素
func (q *Queue) Poll() interface{} {
	q.locker.Lock()
	defer q.locker.Unlock()

	if q.Head == nil {
		return nil
	}
	n := q.Head
	// 代表队列中仅一个元素
	if n.Next == nil {
		q.Head = nil
		q.Tail = nil

	} else {
		q.Head = n.Next
	}
	q.Size--
	return n.Data
}

func (q *Queue) Peek() interface{} {
	q.locker.Lock()
	defer q.locker.Unlock()

	if q.Head == nil {
		return nil
	}

	return q.Head.Data
}

func (q *Queue) PeekTail() interface{} {
	q.locker.Lock()
	defer q.locker.Unlock()

	if q.Tail == nil {
		return nil
	}

	return q.Tail.Data
}

func (q *Queue) Length() int {
	q.locker.Lock()
	defer q.locker.Unlock()
	return q.Size
}
