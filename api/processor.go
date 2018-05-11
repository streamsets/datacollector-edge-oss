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

// Processor is Data Collector Edge processor stage. Processor stages receive records from an origin
// or other processors stages, perform operations on the records and write them out so they can be
// processed by another processor or destination stages.
//
// Produce method - When running a pipeline, the Data Collector Edge calls this method from the Processor stage with a
// batch of records to process.
// Parameter batch - the batch of records to process.
// Parameter batchMaker - records created by the Processor stage must be added to the BatchMaker
// for them to be available to the rest of the pipeline.
type Processor interface {
	Process(batch Batch, batchMaker BatchMaker) error
}
