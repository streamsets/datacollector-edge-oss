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

// Destination is a Data Collector Edge destination stage. Destination stages receive records from origin
// stages and write them to an external system.
//
// Write method, when running a pipeline, the Data Collector Edge calls this method from the Destination stage to write
// a batch of records to an external system.
type Destination interface {
	Write(batch Batch) error
}
