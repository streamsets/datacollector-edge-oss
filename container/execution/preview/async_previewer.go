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
package preview

import (
	"github.com/satori/go.uuid"
	"github.com/streamsets/datacollector-edge/container/execution"
	pipelineStore "github.com/streamsets/datacollector-edge/container/store"
)

type AsyncPreviewer struct {
	syncPreviewer SyncPreviewer
}

func (p *AsyncPreviewer) GetId() string {
	return p.syncPreviewer.GetId()
}

func (p *AsyncPreviewer) ValidateConfigs(timeoutMillis int64) error {
	return nil
}

func (p *AsyncPreviewer) Start(
	batches int,
	batchSize int,
	skipTargets bool,
	stopStage string,
	stagesOverride []execution.StageOutputJson,
	timeoutMillis int64,
	testOrigin bool,
) error {
	go p.syncPreviewer.Start(batches, batchSize, skipTargets, stopStage, stagesOverride, timeoutMillis, testOrigin)
	return nil
}

func (p *AsyncPreviewer) Stop() error {
	return p.syncPreviewer.Stop()
}

func (p *AsyncPreviewer) GetStatus() string {
	return p.syncPreviewer.GetStatus()
}

func (p *AsyncPreviewer) GetOutput() execution.PreviewOutput {
	return p.syncPreviewer.GetOutput()
}

func NewAsyncPreviewer(
	pipelineId string,
	config execution.Config,
	pipelineStoreTask pipelineStore.PipelineStoreTask,
) (execution.Previewer, error) {
	syncPreviewer := &SyncPreviewer{
		pipelineId:        pipelineId,
		previewerId:       uuid.NewV4().String(),
		config:            config,
		pipelineStoreTask: pipelineStoreTask,
		previewOutput:     execution.PreviewOutput{},
	}
	return syncPreviewer, nil
}
