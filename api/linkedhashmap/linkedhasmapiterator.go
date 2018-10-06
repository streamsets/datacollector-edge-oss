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

import "github.com/streamsets/datacollector-edge/api/linkedhashmap/collections"

type Iterator struct {
	m       *Map
	current *Link
}

func (iterator *Iterator) HasNext() bool {
	return !(iterator.current == nil)
}

func (iterator *Iterator) Next() *collections.Entry {
	temp := iterator.current
	if temp == nil {
		return nil
	}
	iterator.current = temp.next
	return collections.NewEntry(temp.key, temp.value)
}

func (iterator *Iterator) Reset() {
	iterator.current = iterator.m.head
}
