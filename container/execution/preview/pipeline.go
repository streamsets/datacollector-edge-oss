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
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
)

type Pipeline struct {
	name              string
	config            execution.Config
	pipelineConf      common.PipelineConfiguration
	pipelineBean      creation.PipelineBean
	pipes             []runner.Pipe
	offsetTracker     execution.SourceOffsetTracker
	errorStageRuntime runner.StageRuntime
	stop              bool
	errorSink         *common.ErrorSink
	eventSink         *common.EventSink
	BatchesOutput     [][]execution.StageOutput
	stagesToSkip      map[string]execution.StageOutputJson
}

func (p *Pipeline) ValidateConfigs() []validation.Issue {
	log.Debug("Preview Pipeline ValidateConfigs()")
	issues := p.Init()
	p.Stop()
	return issues
}

func (p *Pipeline) Init() []validation.Issue {
	log.Debug("Preview Pipeline Init()")
	var issues []validation.Issue
	for _, stagePipe := range p.pipes {
		stageIssues := stagePipe.Init()
		issues = append(issues, stageIssues...)
	}

	errorStageIssues := p.errorStageRuntime.Init()
	issues = append(issues, errorStageIssues...)

	return issues
}

func (p *Pipeline) Run(
	batches int,
	batchSize int,
	skipTargets bool,
	stopStage string,
	stagesOverride []execution.StageOutputJson,
) {
	log.Debug("Preview Pipeline Run()")
	p.BatchesOutput = make([][]execution.StageOutput, batches)
	p.stagesToSkip = make(map[string]execution.StageOutputJson)

	if stagesOverride != nil && len(stagesOverride) > 0 {
		for _, stageOutputJson := range stagesOverride {
			p.stagesToSkip[stageOutputJson.InstanceName] = stageOutputJson
		}
	}

	err := p.runBatch(0, batchSize, skipTargets)
	if err != nil {
		log.WithError(err).Error("Error while processing batch")
		log.Info("Stopping Pipeline")
		p.Stop()
	}
}

func (p *Pipeline) runBatch(batchCount int, batchSize int, skipTargets bool) error {
	p.errorSink.ClearErrorRecordsAndMessages()
	previousOffset := p.offsetTracker.GetOffset()
	pipeBatch := runner.NewFullPipeBatch(p.offsetTracker, batchSize, p.errorSink, p.eventSink, true)

	for _, pipe := range p.pipes {
		if !(skipTargets && pipe.IsTarget()) {
			if stageOutputJson, ok := p.stagesToSkip[pipe.GetInstanceName()]; ok {
				stageOutput, _ := execution.NewStageOutput(pipe.GetStageContext(), stageOutputJson)
				pipeBatch.OverrideStageOutput(pipe, stageOutput)
			} else {
				err := pipe.Process(pipeBatch)
				if err != nil {
					log.WithError(err).Error()
				}
			}
		}
	}

	errorRecords := make([]api.Record, 0)
	for _, stageBean := range p.pipelineBean.Stages {
		errorRecordsForThisStage := p.errorSink.GetStageErrorRecords(stageBean.Config.InstanceName)
		if errorRecordsForThisStage != nil && len(errorRecordsForThisStage) > 0 {
			errorRecords = append(errorRecords, errorRecordsForThisStage...)
		}
	}
	if len(errorRecords) > 0 {
		batch := runner.NewBatchImpl(p.errorStageRuntime.GetInstanceName(), errorRecords, previousOffset)
		_, err := p.errorStageRuntime.Execute(previousOffset, -1, batch, nil)
		if err != nil {
			return err
		}
	}

	p.BatchesOutput[batchCount] = pipeBatch.GetSnapshotsOfAllStagesOutput()

	return nil
}

func (p *Pipeline) Stop() {
	log.Debug("Preview Pipeline Stop()")
	for _, stagePipe := range p.pipes {
		stagePipe.Destroy()
	}
	p.errorStageRuntime.Destroy()
	p.stop = true
}

func NewPreviewPipeline(
	config execution.Config,
	pipelineConfig common.PipelineConfiguration,
) (*Pipeline, []validation.Issue) {
	issues := make([]validation.Issue, 0)
	metricRegistry := metrics.NewRegistry()
	sourceOffsetTracker := NewPreviewSourceOffsetTracker(pipelineConfig.PipelineId)
	pipelineConfigForParam := creation.NewPipelineConfigBean(pipelineConfig)
	stageRuntimeList := make([]runner.StageRuntime, len(pipelineConfig.Stages))
	pipes := make([]runner.Pipe, len(pipelineConfig.Stages))
	errorSink := common.NewErrorSink()
	eventSink := common.NewEventSink()

	var errorStageRuntime runner.StageRuntime

	var resolvedParameters = pipelineConfigForParam.Constants

	pipelineBean, issues := creation.NewPipelineBean(pipelineConfig, resolvedParameters)
	if len(issues) > 0 {
		return nil, issues
	}

	for i, stageBean := range pipelineBean.Stages {
		var services map[string]api.Service
		if stageBean.Services != nil && len(stageBean.Services) > 0 {
			services = make(map[string]api.Service)
			for _, serviceBean := range stageBean.Services {
				services[serviceBean.Config.Service] = serviceBean.Service
			}
		}

		stageContext, err := common.NewStageContext(
			stageBean.Config,
			resolvedParameters,
			metricRegistry,
			errorSink,
			false,
			pipelineConfigForParam.ErrorRecordPolicy,
			services,
			pipelineBean.ElContext,
			eventSink,
			true,
		)
		if err != nil {
			issues = append(issues, validation.Issue{
				InstanceName: stageBean.Config.InstanceName,
				Level:        common.StageConfig,
				Count:        1,
				Message:      err.Error(),
			})
			return nil, issues
		}
		stageRuntimeList[i] = runner.NewStageRuntime(pipelineBean, stageBean, stageContext)
		pipes[i] = runner.NewStagePipe(stageRuntimeList[i], config)
	}

	log.Debug("Error Stage:", pipelineBean.ErrorStage.Config.InstanceName)
	errorStageContext, err := common.NewStageContext(
		pipelineBean.ErrorStage.Config,
		resolvedParameters,
		metricRegistry,
		errorSink,
		true,
		pipelineConfigForParam.ErrorRecordPolicy,
		nil,
		pipelineBean.ElContext,
		eventSink,
		true,
	)
	if err != nil {
		issues = append(issues, validation.Issue{
			InstanceName: pipelineBean.ErrorStage.Config.InstanceName,
			Level:        common.StageConfig,
			Count:        1,
			Message:      err.Error(),
		})
		return nil, issues
	}
	errorStageRuntime = runner.NewStageRuntime(pipelineBean, pipelineBean.ErrorStage, errorStageContext)

	p := &Pipeline{
		pipelineConf:      pipelineConfig,
		pipelineBean:      pipelineBean,
		pipes:             pipes,
		errorStageRuntime: errorStageRuntime,
		errorSink:         errorSink,
		eventSink:         eventSink,
		offsetTracker:     sourceOffsetTracker,
	}

	return p, issues
}
