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

// Origin is Data Collector Edge origin stage. Origin stages consume data from an external system
// creating records that can be processed by processor or destination stages.
//
// Produce method - When running a pipeline, the Data Collector Edge calls this method from the Origin stage to obtain
// a batch of records for processing.
// Origin stages should not block indefinitely within this method if there is no data. They should
// have an internal timeout after which they produce an empty batch. By doing so it gives the chance to other
// stages in pipeline to know that the pipeline is still healthy but there is no data coming; and potentially
// allowing notifications to external systems.
// lastSourceOffset the offset returned by the previous call to this method, or NULL if
// this method is being called for the first time ever.
// maxBatchSize the requested maximum batch size a single call to this method should produce.
// batchMaker records created by the Source stage must be added to the
// BatchMaker for them to be available to the rest of the pipeline.
// Return the offset for the next call to this method. If NULL is returned it means the
// Source stage has fully process that data, that no more data is to be expected and that the
// pipeline should finish once the current batch is fully processed.
// Return error if the Source had an error while consuming data or creating records.
type Origin interface {
	Produce(lastSourceOffset *string, maxBatchSize int, batchMaker BatchMaker) (*string, error)
}
