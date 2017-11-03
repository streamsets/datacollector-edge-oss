/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package manager

import (
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/execution"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"github.com/streamsets/datacollector-edge/container/store"
)

type PipelineManager struct {
	config            execution.Config
	runnerMap         map[string]*runner.StandaloneRunner
	runtimeInfo       *common.RuntimeInfo
	pipelineStoreTask store.PipelineStoreTask
}

func (p *PipelineManager) GetRunner(pipelineId string) *runner.StandaloneRunner {
	if p.runnerMap[pipelineId] == nil {
		pRunner, err := runner.NewStandaloneRunner(pipelineId, p.config, p.runtimeInfo, p.pipelineStoreTask)
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
		runnerMap:         make(map[string]*runner.StandaloneRunner),
		runtimeInfo:       runtimeInfo,
		pipelineStoreTask: pipelineStoreTask,
	}

	return &pipelineManager, nil
}
