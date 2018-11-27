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

import (
	"context"
	"github.com/rcrowley/go-metrics"
	"github.com/streamsets/datacollector-edge/api/validation"
)

type StageContext interface {
	// If we plan to support ELs later, we should remove and provide in build support for this
	GetResolvedValue(configValue interface{}) (interface{}, error)
	CreateRecord(recordSourceId string, value interface{}) (Record, error)
	CreateEventRecord(recordSourceId string, value interface{}, eventType string, eventVersion int) (Record, error)
	GetMetrics() metrics.Registry
	ToError(err error, record Record)
	ToEvent(record Record)
	ReportError(err error)
	GetOutputLanes() []string
	Evaluate(value string, configName string, ctx context.Context) (interface{}, error)
	IsErrorStage() bool
	CreateConfigIssue(error string, optional ...interface{}) validation.Issue
	GetService(serviceName string) (Service, error)
	IsPreview() bool
	GetPipelineParameters() map[string]interface{}
	SetStop()
	IsStopped() bool
}
