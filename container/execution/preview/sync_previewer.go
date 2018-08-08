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
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/execution"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	pipelineStore "github.com/streamsets/datacollector-edge/container/store"
)

const (
	CREATED = "CREATED" // The preview was just created and
	// nothing else has happened yet
	Validating      = "VALIDATING"       // validating the configuration, during preview
	Valid           = "VALID"            // configuration is valid, during preview
	InValid         = "INVALID"          // configuration is invalid, during preview
	ValidationError = "VALIDATION_ERROR" // validation failed with an exception, during validation
	Starting        = "STARTING"         // preview starting (initialization)
	StartError      = "START_ERROR"      // preview failed while start (during initialization)
	Running         = "RUNNING"          // preview running
	RunError        = "RUN_ERROR"        // preview failed while running
	Finishing       = "FINISHING"        // preview finishing (calling destroy on pipeline)
	Finished        = "FINISHED"         // preview finished  (done)
	Cancelling      = "CANCELLING"       // preview has been manually stopped
	Cancelled       = "CANCELLED"        // preview has been manually stopped
	TimingOut       = "TIMING_OUT"       // preview/validate time out
	TimedOut        = "TIMED_OUT"        // preview/validate time out
)

type SyncPreviewer struct {
	pipelineId           string
	previewerId          string
	previewOutput        execution.PreviewOutput
	config               execution.Config
	pipelineConfig       common.PipelineConfiguration
	previewPipeline      *Pipeline
	metricsEventRunnable *runner.MetricsEventRunnable
	pipelineStoreTask    pipelineStore.PipelineStoreTask
}

func (p *SyncPreviewer) GetId() string {
	return p.previewerId
}

func (p *SyncPreviewer) ValidateConfigs(timeoutMillis int64) error {
	p.previewOutput.PreviewStatus = Validating
	var err error
	p.pipelineConfig, err = p.pipelineStoreTask.LoadPipelineConfig(p.pipelineId)
	if err != nil {
		p.previewOutput.PreviewStatus = ValidationError
		p.previewOutput.Message = err.Error()
		return err
	}

	previewPipeline, issues := NewPreviewPipeline(p.config, p.pipelineConfig)
	if len(issues) > 0 {
		p.previewOutput.PreviewStatus = ValidationError
		p.previewOutput.Issues = validation.NewIssues(issues)
		return err
	}

	issues = previewPipeline.ValidateConfigs()
	p.previewOutput.Issues = validation.NewIssues(issues)
	if len(issues) > 0 {
		p.previewOutput.PreviewStatus = InValid
	} else {
		p.previewOutput.PreviewStatus = Valid
	}

	return nil
}

func (p *SyncPreviewer) Start(
	batches int,
	batchSize int,
	skipTargets bool,
	stopStage string,
	stagesOverride []execution.StageOutputJson,
	timeoutMillis int64,
	testOrigin bool,
) error {
	p.previewOutput.PreviewStatus = Starting
	var err error
	p.pipelineConfig, err = p.pipelineStoreTask.LoadPipelineConfig(p.pipelineId)
	if err != nil {
		p.previewOutput.Message = err.Error()
		return err
	}

	if testOrigin && p.pipelineConfig.TestOriginStage != nil {
		p.pipelineConfig.Stages[0] = p.pipelineConfig.TestOriginStage
	}

	previewPipeline, issues := NewPreviewPipeline(p.config, p.pipelineConfig)
	if len(issues) > 0 {
		p.previewOutput.PreviewStatus = StartError
		p.previewOutput.Issues = validation.NewIssues(issues)
		return err
	}

	issues = previewPipeline.Init()
	if len(issues) > 0 {
		p.previewOutput.PreviewStatus = InValid
		p.previewOutput.Issues = validation.NewIssues(issues)
		return nil
	} else {
		p.previewOutput.PreviewStatus = Running
	}

	previewPipeline.Run(batches, batchSize, skipTargets, stopStage, stagesOverride)

	previewOutput, err := execution.NewPreviewOutput(previewPipeline.BatchesOutput)
	if err != nil {
		p.previewOutput.PreviewStatus = RunError
		p.previewOutput.Message = err.Error()
		return err
	}

	p.previewOutput.Output = previewOutput

	p.previewOutput.PreviewStatus = Finishing

	previewPipeline.Stop()
	p.previewOutput.PreviewStatus = Finished
	return nil
}

func (p *SyncPreviewer) Stop() error {
	return nil
}

func (p *SyncPreviewer) GetStatus() string {
	return p.previewOutput.PreviewStatus
}

func (p *SyncPreviewer) GetOutput() execution.PreviewOutput {
	return p.previewOutput
}
