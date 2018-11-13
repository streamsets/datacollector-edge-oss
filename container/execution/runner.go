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
package execution

import (
	"github.com/rcrowley/go-metrics"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
)

type Runner interface {
	GetPipelineConfig() common.PipelineConfiguration
	GetStatus() (*common.PipelineState, error)
	GetHistory() ([]*common.PipelineState, error)
	GetMetrics() (metrics.Registry, error)
	StartPipeline(runtimeParameters map[string]interface{}) (*common.PipelineState, error)
	StopPipeline() (*common.PipelineState, error)
	ResetOffset() error
	CommitOffset(sourceOffset common.SourceOffset) error
	GetOffset() (common.SourceOffset, error)
	IsRemotePipeline() bool
	GetErrorRecords(stageInstanceName string, size int) ([]api.Record, error)
	GetErrorMessages(stageInstanceName string, size int) ([]api.ErrorMessage, error)
}
