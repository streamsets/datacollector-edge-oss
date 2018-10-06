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
package collections

// Iterator is stateful iterator for ordered collections
type Iterator interface {
	// HasNext method moves the iterator to the next element and returns true if there was a next
	// element in the collection.
	//
	// If HasNext() returns true, then next element's entry can be retrieved by Next().
	//
	// If Next() was called for the first time, then it will point the iterator to the first
	// element if it exists.
	//
	// Modifies the state of the iterator.
	HasNext() bool

	// Next method returns the next element entry if it exists
	Next() *Entry

	// Reset method resets the iterator to its initial state. Call Next() to fetch the first
	// element if any.
	Reset()
}
