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

import "github.com/streamsets/datacollector-edge/api/validation"

// Stage is the base interface for Data Collector Edge stages implementations defining their common context and lifecycle.
//
// Init method initializes the stage.
// This method is called once when the pipeline is being initialized before the processing any data.
// If the stage returns an empty list of ConfigIssue then the stage is considered ready to process data.
// Else it is considered it is mis-configured or that there is a problem and the stage is not ready to process data,
// thus aborting the pipeline initialization.
//
// Destroy method destroys the stage. It should be used to release any resources held by the stage after initialization
// or processing.
// This method is called once when the pipeline is being shutdown. After this method is called, the stage will not
// be called to process any more data.
// This method is also called after a failed initialization to allow releasing resources created before the
// initialization failed.
type Stage interface {
	Init(stageContext StageContext) []validation.Issue
	Destroy() error
}
