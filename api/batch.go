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

package api

// Batch is the interface that wraps the basic Batch method.
//
// GetSourceOffset returns the initial offset of the current batch.
// This return value should be treated as an opaque value as it is source dependent.
//
// GetRecords returns an iterator with all the records in the batch for the current stage.
// Every time this method is called it returns a new iterator with all records in the batch.
type Batch interface {
	GetSourceOffset() *string
	GetRecords() []Record
}
