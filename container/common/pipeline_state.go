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
package common

const (
	EDITED        = "EDITED"        // pipeline job has been create/modified, didn't run since the creation/modification
	STARTING      = "STARTING"      // pipeline job starting (initialization)
	START_ERROR   = "START_ERROR"   // pipeline job failed while start (during initialization)
	RUNNING       = "RUNNING"       // pipeline job running
	RUNNING_ERROR = "RUNNING_ERROR" // pipeline job failed while running (calling destroy on pipeline)
	RUN_ERROR     = "RUN_ERROR"     // pipeline job failed while running (done)
	FINISHING     = "FINISHING"     // pipeline job finishing (source reached end, returning NULL offset) (calling destroy on pipeline)
	FINISHED      = "FINISHED"      // pipeline job finished
	RETRY         = "RETRY"         // pipeline job retrying
	STOPPING      = "STOPPING"      // pipeline job has been manually stopped (calling destroy on pipeline)
	STOPPED       = "STOPPED"       // pipeline job has been manually stopped (done)
)

type PipelineState struct {
	PipelineId string                 `json:"pipelineId"`
	Status     string                 `json:"status"`
	Message    string                 `json:"message"`
	TimeStamp  int64                  `json:"timeStamp"`
	Attributes map[string]interface{} `json:"attributes"`
	Metrics    string                 `json:"metrics"`
}
