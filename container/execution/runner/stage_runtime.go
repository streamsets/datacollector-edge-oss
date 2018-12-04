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
package runner

import (
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
)

type StageRuntime struct {
	pipelineBean creation.PipelineBean
	config       *common.StageConfiguration
	stageBean    creation.StageBean
	stageContext api.StageContext
}

func (s *StageRuntime) Init() []validation.Issue {
	issues := make([]validation.Issue, 0)
	if s.stageBean.Services != nil {
		for _, serviceBean := range s.stageBean.Services {
			serviceIssues := serviceBean.Service.Init(s.stageContext)
			issues = append(issues, serviceIssues...)
		}
	}
	stageIssues := s.stageBean.Stage.Init(s.stageContext)
	return append(issues, stageIssues...)
}

func (s *StageRuntime) Execute(
	previousOffset *string,
	batchSize int,
	batch *BatchImpl,
	batchMaker *BatchMakerImpl,
) (*string, error) {
	var newOffset *string
	var err error
	if s.stageBean.IsSource() {
		newOffset, err = s.stageBean.Stage.(api.Origin).Produce(previousOffset, batchSize, batchMaker)
	} else if s.stageBean.IsProcessor() {
		err = s.stageBean.Stage.(api.Processor).Process(batch, batchMaker)
	} else if s.stageBean.IsTarget() {
		err = s.stageBean.Stage.(api.Destination).Write(batch)
	}
	return newOffset, err
}

func (s *StageRuntime) Destroy() {
	if s.stageBean.Services != nil {
		for _, serviceBean := range s.stageBean.Services {
			_ = serviceBean.Service.Destroy()
		}
	}
	_ = s.stageBean.Stage.Destroy()
}

func (s *StageRuntime) GetInstanceName() string {
	return s.config.InstanceName
}

func NewStageRuntime(
	pipelineBean creation.PipelineBean,
	stageBean creation.StageBean,
	stageContext api.StageContext,
) StageRuntime {
	return StageRuntime{
		pipelineBean: pipelineBean,
		config:       stageBean.Config,
		stageBean:    stageBean,
		stageContext: stageContext,
	}
}
