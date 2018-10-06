// Copyright 2018 StreamSets Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package linkedhashmap

import (
	"fmt"
	"github.com/streamsets/datacollector-edge/api/linkedhashmap/collections"
	"strings"
)

type Link struct {
	key   interface{}
	value interface{}
	next  *Link
	prev  *Link
}

type Map struct {
	m    map[interface{}]*Link
	head *Link
	tail *Link
}

func newLink(key interface{}, value interface{}) *Link {
	return &Link{key: key, value: value, next: nil, prev: nil}
}

func New() *Map {
	return &Map{m: make(map[interface{}]*Link), head: nil, tail: nil}
}

func (m *Map) Put(key interface{}, value interface{}) {
	link, found := m.m[key]
	if !found {
		link = newLink(key, value)
		if m.tail == nil {
			m.head = link
			m.tail = link
		} else {
			m.tail.next = link
			link.prev = m.tail
			m.tail = link
		}
		m.m[key] = link
	} else {
		link.value = value
	}
}

func (m *Map) Get(key interface{}) (value interface{}, found bool) {
	var link *Link
	link, found = m.m[key]
	if found {
		value = link.value
	} else {
		value = nil
	}
	return
}

func (m *Map) Remove(key interface{}) {
	link, found := m.m[key]
	if found {
		delete(m.m, key)
		if m.head == link && m.tail == link {
			m.head = nil
			m.tail = nil
		} else if m.tail == link {
			m.tail = link.prev
			link.prev.next = nil
		} else if m.head == link {
			m.head = link.next
			link.next.prev = nil
		} else {
			link.prev.next = link.next
			link.next.prev = link.prev
		}
	}
}

func (m *Map) IsEmpty() bool {
	return m.Size() == 0
}

func (m *Map) Size() int {
	return len(m.m)
}

func (m *Map) Keys() []interface{} {
	keys := make([]interface{}, m.Size())
	count := 0
	for current := m.head; current != nil; current = current.next {
		keys[count] = current.key
		count++
	}
	return keys
}

func (m *Map) Values() []interface{} {
	values := make([]interface{}, m.Size())
	count := 0
	for current := m.head; current != nil; current = current.next {
		values[count] = current.value
		count++
	}
	return values
}

func (m *Map) Contains(keys ...interface{}) bool {
	for _, key := range keys {
		_, found := m.m[key]
		if !found {
			return false
		}
	}
	return true
}

func (m *Map) Clear() {
	m.m = make(map[interface{}]*Link)
	m.head = nil
	m.tail = nil
}

func (m *Map) Iterator() collections.Iterator {
	return &Iterator{m: m, current: m.head}
}

func (m *Map) String() string {
	str := "LinkedHashMap\nmap["
	for current := m.head; current != nil; current = current.next {
		str += fmt.Sprintf("%v:%v ", current.key, current.value)
	}
	return strings.TrimRight(str, " ") + "]"
}
