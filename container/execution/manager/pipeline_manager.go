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
package manager

import (
	"errors"
	"fmt"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/execution"
	"github.com/streamsets/datacollector-edge/container/execution/preview"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"github.com/streamsets/datacollector-edge/container/store"
)

type PipelineManager struct {
	config            execution.Config
	runnerMap         map[string]execution.Runner
	previewerMap      map[string]execution.Previewer
	runtimeInfo       *common.RuntimeInfo
	pipelineStoreTask store.PipelineStoreTask
}

func (p *PipelineManager) CreatePreviewer(pipelineId string) (execution.Previewer, error) {
	previewer, err := preview.NewAsyncPreviewer(pipelineId, p.config, p.pipelineStoreTask)
	if err != nil {
		return nil, err
	}
	p.previewerMap[previewer.GetId()] = previewer
	return previewer, nil
}

func (p *PipelineManager) GetPreviewer(previewerId string) (execution.Previewer, error) {
	if p.previewerMap[previewerId] == nil {
		return nil, errors.New(fmt.Sprintf("Cannot find the previewer in cache for id: %s", previewerId))
	}
	return p.previewerMap[previewerId], nil
}

func (p *PipelineManager) GetRunner(pipelineId string) execution.Runner {
	if p.runnerMap[pipelineId] == nil {
		pRunner, err := runner.NewEdgeRunner(pipelineId, p.config, p.runtimeInfo, p.pipelineStoreTask)
		if err != nil {
			panic(err)
		}
		p.runnerMap[pipelineId] = pRunner
	}
	return p.runnerMap[pipelineId]
}

func (p *PipelineManager) StartPipeline(
	pipelineId string,
	runtimeParameters map[string]interface{},
) (*common.PipelineState, error) {
	return p.GetRunner(pipelineId).StartPipeline(runtimeParameters)
}

func (p *PipelineManager) StopPipeline(pipelineId string) (*common.PipelineState, error) {
	return p.GetRunner(pipelineId).StopPipeline()
}

func (p *PipelineManager) ResetOffset(pipelineId string) error {
	return p.GetRunner(pipelineId).ResetOffset()
}

func NewManager(
	config execution.Config,
	runtimeInfo *common.RuntimeInfo,
	pipelineStoreTask store.PipelineStoreTask,
) (Manager, error) {
	pipelineManager := PipelineManager{
		config:            config,
		runnerMap:         make(map[string]execution.Runner),
		previewerMap:      make(map[string]execution.Previewer),
		runtimeInfo:       runtimeInfo,
		pipelineStoreTask: pipelineStoreTask,
	}
	return &pipelineManager, nil
}
