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
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution"
	"github.com/streamsets/datacollector-edge/container/util"
	"time"
)

type Pipeline struct {
	name              string
	config            execution.Config
	pipelineConf      common.PipelineConfiguration
	pipelineBean      creation.PipelineBean
	pipes             []Pipe
	errorStageRuntime StageRuntime
	offsetTracker     execution.SourceOffsetTracker
	stop              bool
	errorSink         *common.ErrorSink
	eventSink         *common.EventSink

	MetricRegistry              metrics.Registry
	batchProcessingTimer        metrics.Timer
	batchCountCounter           metrics.Counter
	batchInputRecordsCounter    metrics.Counter
	batchOutputRecordsCounter   metrics.Counter
	batchErrorRecordsCounter    metrics.Counter
	batchErrorMessagesCounter   metrics.Counter
	batchCountMeter             metrics.Meter
	batchInputRecordsMeter      metrics.Meter
	batchOutputRecordsMeter     metrics.Meter
	batchErrorRecordsMeter      metrics.Meter
	batchErrorMessagesMeter     metrics.Meter
	batchInputRecordsHistogram  metrics.Histogram
	batchOutputRecordsHistogram metrics.Histogram
	batchErrorRecordsHistogram  metrics.Histogram
	batchErrorMessagesHistogram metrics.Histogram

	stageToErrorRecordsMap  map[string][]api.Record
	stageToErrorMessagesMap map[string][]api.ErrorMessage
}

const (
	AtMostOnce                    = "AT_MOST_ONCE"
	AtLeastOnce                   = "AT_LEAST_ONCE"
	PipelineBatchProcessing       = "pipeline.batchProcessing"
	PipelineBatchCount            = "pipeline.batchCount"
	PipelineBatchInputRecords     = "pipeline.batchInputRecords"
	PipelineBatchOutputRecords    = "pipeline.batchOutputRecords"
	PipelineBatchErrorRecords     = "pipeline.batchErrorRecords"
	PipelineBatchErrorMessages    = "pipeline.batchErrorMessages"
	PipelineInputRecordsPerBatch  = "pipeline.inputRecordsPerBatch"
	PipelineOutputRecordsPerBatch = "pipeline.outputRecordsPerBatch"
	PipelineErrorRecordsPerBatch  = "pipeline.errorRecordsPerBatch"
	PipelineErrorsPerBatch        = "pipeline.errorsPerBatch"
	MaxCountInCache               = 10
)

func (p *Pipeline) Init() []validation.Issue {
	var issues []validation.Issue
	for _, stagePipe := range p.pipes {
		stageIssues := stagePipe.Init()
		issues = append(issues, stageIssues...)
	}

	errorStageIssues := p.errorStageRuntime.Init()
	issues = append(issues, errorStageIssues...)

	return issues
}

func (p *Pipeline) Run() {
	log.Debug("Pipeline Run()")

	defer func() {
		for _, stagePipe := range p.pipes {
			stagePipe.Destroy()
		}
		p.errorStageRuntime.Destroy()
	}()

	for !p.offsetTracker.IsFinished() && !p.stop {
		err := p.runBatch()
		if err != nil {
			log.WithError(err).Error("Error while processing batch")
			log.Info("Stopping Pipeline")
			p.Stop()
		}
	}
}

func (p *Pipeline) runBatch() error {
	committed := false
	start := time.Now()

	p.errorSink.ClearErrorRecordsAndMessages()
	p.eventSink.ClearEventRecords()

	previousOffset := p.offsetTracker.GetOffset()

	pipeBatch := NewFullPipeBatch(p.offsetTracker, p.config.MaxBatchSize, p.errorSink, p.eventSink, false)

	for _, pipe := range p.pipes {
		if p.pipelineBean.Config.DeliveryGuarantee == AtMostOnce &&
			pipe.IsTarget() && // if destination
			!committed {
			if err := p.offsetTracker.CommitOffset(); err != nil {
				return err
			}
			committed = true
		}

		err := pipe.Process(pipeBatch)
		if err != nil {
			log.WithError(err).Error()
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
		batch := NewBatchImpl(p.errorStageRuntime.config.InstanceName, errorRecords, previousOffset)
		_, err := p.errorStageRuntime.Execute(previousOffset, -1, batch, nil)
		if err != nil {
			return err
		}
	}

	if p.pipelineBean.Config.DeliveryGuarantee == AtLeastOnce {
		p.offsetTracker.CommitOffset()
	}

	p.batchProcessingTimer.UpdateSince(start)
	p.batchCountCounter.Inc(1)
	p.batchCountMeter.Mark(1)

	p.batchInputRecordsCounter.Inc(pipeBatch.GetInputRecords())
	p.batchOutputRecordsCounter.Inc(pipeBatch.GetOutputRecords())
	p.batchErrorMessagesCounter.Inc(pipeBatch.GetErrorMessages())
	p.batchErrorRecordsCounter.Inc(pipeBatch.GetErrorRecords())

	p.batchInputRecordsMeter.Mark(pipeBatch.GetInputRecords())
	p.batchOutputRecordsMeter.Mark(pipeBatch.GetOutputRecords())
	p.batchErrorMessagesMeter.Mark(pipeBatch.GetErrorMessages())
	p.batchErrorRecordsMeter.Mark(pipeBatch.GetErrorRecords())

	p.batchInputRecordsHistogram.Update(pipeBatch.GetInputRecords())
	p.batchOutputRecordsHistogram.Update(pipeBatch.GetOutputRecords())
	p.batchErrorMessagesHistogram.Update(pipeBatch.GetErrorMessages())
	p.batchErrorRecordsHistogram.Update(pipeBatch.GetErrorRecords())

	// Retain X number of error records and error messages per stage
	p.retainErrorRecordsInMemory(pipeBatch.GetErrorSink().GetErrorRecords())
	p.retainErrorMessagesInMemory(pipeBatch.GetErrorSink().GetErrorMessages())

	return nil
}

func (p *Pipeline) retainErrorRecordsInMemory(stageErrorRecords map[string][]api.Record) {
	for stageInstanceName, errorRecords := range stageErrorRecords {
		if _, keyExists := p.stageToErrorRecordsMap[stageInstanceName]; !keyExists {
			p.stageToErrorRecordsMap[stageInstanceName] = make([]api.Record, 0, MaxCountInCache)
		}

		for _, record := range errorRecords {
			if len(p.stageToErrorRecordsMap[stageInstanceName]) >= MaxCountInCache {
				p.stageToErrorRecordsMap[stageInstanceName] = append(p.stageToErrorRecordsMap[stageInstanceName][1:], record)
			} else {
				p.stageToErrorRecordsMap[stageInstanceName] = append(p.stageToErrorRecordsMap[stageInstanceName], record)
			}
		}
	}
}

func (p *Pipeline) retainErrorMessagesInMemory(stageErrorMessages map[string][]api.ErrorMessage) {
	for stageInstanceName, errorMessages := range stageErrorMessages {
		if _, keyExists := p.stageToErrorMessagesMap[stageInstanceName]; !keyExists {
			p.stageToErrorMessagesMap[stageInstanceName] = make([]api.ErrorMessage, 0, MaxCountInCache)
		}

		for _, errorMessage := range errorMessages {
			if len(p.stageToErrorMessagesMap[stageInstanceName]) >= MaxCountInCache {
				p.stageToErrorMessagesMap[stageInstanceName] = append(p.stageToErrorMessagesMap[stageInstanceName][1:], errorMessage)
			} else {
				p.stageToErrorMessagesMap[stageInstanceName] = append(p.stageToErrorMessagesMap[stageInstanceName], errorMessage)
			}
		}
	}
}

func (p *Pipeline) GetErrorRecords(stageInstanceName string, size int) ([]api.Record, error) {
	return p.stageToErrorRecordsMap[stageInstanceName], nil
}

func (p *Pipeline) GetErrorMessages(stageInstanceName string, size int) ([]api.ErrorMessage, error) {
	return p.stageToErrorMessagesMap[stageInstanceName], nil
}

func (p *Pipeline) Stop() {
	log.Debug("Pipeline Stop()")
	p.stop = true
	for _, pipe := range p.pipes {
		pipe.GetStageContext().SetStop()
	}
}

func NewPipeline(
	config execution.Config,
	pipelineConfig common.PipelineConfiguration,
	sourceOffsetTracker execution.SourceOffsetTracker,
	runtimeParameters map[string]interface{},
	metricRegistry metrics.Registry,
) (*Pipeline, []validation.Issue) {
	issues := make([]validation.Issue, 0)
	pipelineConfigForParam := creation.NewPipelineConfigBean(pipelineConfig)
	stageRuntimeList := make([]StageRuntime, len(pipelineConfig.Stages))
	pipes := make([]Pipe, len(pipelineConfig.Stages))
	errorSink := common.NewErrorSink()
	eventSink := common.NewEventSink()

	var errorStageRuntime StageRuntime

	var resolvedParameters = make(map[string]interface{})
	for k, v := range pipelineConfigForParam.Constants {
		if runtimeParameters != nil && runtimeParameters[k] != nil {
			resolvedParameters[k] = runtimeParameters[k]
		} else {
			resolvedParameters[k] = v
		}
	}

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
			false,
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
		stageRuntimeList[i] = NewStageRuntime(pipelineBean, stageBean, stageContext)
		pipes[i] = NewStagePipe(stageRuntimeList[i], config)
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
		false,
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
	errorStageRuntime = NewStageRuntime(pipelineBean, pipelineBean.ErrorStage, errorStageContext)

	p := &Pipeline{
		pipelineConf:      pipelineConfig,
		pipelineBean:      pipelineBean,
		pipes:             pipes,
		errorStageRuntime: errorStageRuntime,
		errorSink:         errorSink,
		eventSink:         eventSink,
		offsetTracker:     sourceOffsetTracker,
		MetricRegistry:    metricRegistry,
		config:            config,
	}

	p.batchProcessingTimer = util.CreateTimer(metricRegistry, PipelineBatchProcessing)

	p.batchCountCounter = util.CreateCounter(metricRegistry, PipelineBatchCount)
	p.batchInputRecordsCounter = util.CreateCounter(metricRegistry, PipelineBatchInputRecords)
	p.batchOutputRecordsCounter = util.CreateCounter(metricRegistry, PipelineBatchOutputRecords)
	p.batchErrorRecordsCounter = util.CreateCounter(metricRegistry, PipelineBatchErrorRecords)
	p.batchErrorMessagesCounter = util.CreateCounter(metricRegistry, PipelineBatchErrorMessages)

	p.batchCountMeter = util.CreateMeter(metricRegistry, PipelineBatchCount)
	p.batchInputRecordsMeter = util.CreateMeter(metricRegistry, PipelineBatchInputRecords)
	p.batchOutputRecordsMeter = util.CreateMeter(metricRegistry, PipelineBatchOutputRecords)
	p.batchErrorRecordsMeter = util.CreateMeter(metricRegistry, PipelineBatchErrorRecords)
	p.batchErrorMessagesMeter = util.CreateMeter(metricRegistry, PipelineBatchErrorMessages)

	p.batchInputRecordsHistogram = util.CreateHistogram5Min(metricRegistry, PipelineInputRecordsPerBatch)
	p.batchOutputRecordsHistogram = util.CreateHistogram5Min(metricRegistry, PipelineOutputRecordsPerBatch)
	p.batchErrorRecordsHistogram = util.CreateHistogram5Min(metricRegistry, PipelineErrorRecordsPerBatch)
	p.batchErrorMessagesHistogram = util.CreateHistogram5Min(metricRegistry, PipelineErrorsPerBatch)

	p.stageToErrorRecordsMap = make(map[string][]api.Record)
	p.stageToErrorMessagesMap = make(map[string][]api.ErrorMessage)

	return p, issues
}
