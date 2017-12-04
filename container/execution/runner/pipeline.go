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
package runner

import (
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution"
	"github.com/streamsets/datacollector-edge/container/util"
	"github.com/streamsets/datacollector-edge/container/validation"
	"time"
)

type Pipeline struct {
	name              string
	config            execution.Config
	standaloneRunner  *StandaloneRunner
	pipelineConf      common.PipelineConfiguration
	pipelineBean      creation.PipelineBean
	pipes             []Pipe
	errorStageRuntime StageRuntime
	offsetTracker     SourceOffsetTracker
	stop              bool
	errorSink         *common.ErrorSink

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
}

const (
	AT_MOST_ONCE                      = "AT_MOST_ONCE"
	AT_LEAST_ONCE                     = "AT_LEAST_ONCE"
	PIPELINE_BATCH_PROCESSING         = "pipeline.batchProcessing"
	PIPELINE_BATCH_COUNT              = "pipeline.batchCount"
	PIPELINE_BATCH_INPUT_RECORDS      = "pipeline.batchInputRecords"
	PIPELINE_BATCH_OUTPUT_RECORDS     = "pipeline.batchOutputRecords"
	PIPELINE_BATCH_ERROR_RECORDS      = "pipeline.batchErrorRecords"
	PIPELINE_BATCH_ERROR_MESSAGES     = "pipeline.batchErrorMessages"
	PIPELINE_INPUT_RECORDS_PER_BATCH  = "pipeline.inputRecordsPerBatch"
	PIPELINE_OUTPUT_RECORDS_PER_BATCH = "pipeline.outputRecordsPerBatch"
	PIPELINE_ERROR_RECORDS_PER_BATCH  = "pipeline.errorRecordsPerBatch"
	PIPELINE_ERRORS_PER_BATCH         = "pipeline.errorsPerBatch"
)

func (p *Pipeline) Init() []validation.Issue {
	var issues []validation.Issue
	for _, stagePipe := range p.pipes {
		stageIssues := stagePipe.Init()
		issues = append(issues, stageIssues...)
	}

	errorStageissues := p.errorStageRuntime.Init()
	issues = append(issues, errorStageissues...)

	return issues
}

func (p *Pipeline) Run() {
	log.Debug("Pipeline Run()")

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

	p.errorSink.ClearErrorRecordsAndMesssages()

	previousOffset := p.offsetTracker.GetOffset()

	pipeBatch := NewFullPipeBatch(p.offsetTracker, 1, p.errorSink)

	for _, pipe := range p.pipes {
		if p.pipelineBean.Config.DeliveryGuarantee == AT_MOST_ONCE &&
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

	if p.pipelineBean.Config.DeliveryGuarantee == AT_LEAST_ONCE {
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

	return nil
}

func (p *Pipeline) Stop() {
	log.Debug("Pipeline Stop()")
	for _, stagePipe := range p.pipes {
		stagePipe.Destroy()
	}
	p.errorStageRuntime.Destroy()
	p.stop = true
}

func NewPipeline(
	config execution.Config,
	standaloneRunner *StandaloneRunner,
	sourceOffsetTracker SourceOffsetTracker,
	runtimeParameters map[string]interface{},
	metricRegistry metrics.Registry,
) (*Pipeline, error) {

	pipelineConfigForParam := creation.NewPipelineConfigBean(standaloneRunner.GetPipelineConfig())
	stageRuntimeList := make([]StageRuntime, len(standaloneRunner.pipelineConfig.Stages))
	pipes := make([]Pipe, len(standaloneRunner.pipelineConfig.Stages))
	errorSink := common.NewErrorSink()

	var errorStageRuntime StageRuntime

	var resolvedParameters = make(map[string]interface{})
	for k, v := range pipelineConfigForParam.Constants {
		if runtimeParameters != nil && runtimeParameters[k] != nil {
			resolvedParameters[k] = runtimeParameters[k]
		} else {
			resolvedParameters[k] = v
		}
	}

	pipelineBean, err := creation.NewPipelineBean(standaloneRunner.GetPipelineConfig(), resolvedParameters)
	if err != nil {
		return nil, err
	}

	for i, stageBean := range pipelineBean.Stages {
		stageContext := &common.StageContextImpl{
			StageConfig: stageBean.Config,
			Parameters:  resolvedParameters,
			Metrics:     metricRegistry,
			ErrorSink:   errorSink,
			ErrorStage:  false,
		}
		stageRuntimeList[i] = NewStageRuntime(pipelineBean, stageBean, stageContext)
		pipes[i] = NewStagePipe(stageRuntimeList[i], config)
	}

	log.Debug("Error Stage:", pipelineBean.ErrorStage.Config.InstanceName)
	errorStageContext := &common.StageContextImpl{
		StageConfig: pipelineBean.ErrorStage.Config,
		Parameters:  resolvedParameters,
		Metrics:     metricRegistry,
		ErrorSink:   errorSink,
		ErrorStage:  true,
	}
	errorStageRuntime = NewStageRuntime(pipelineBean, pipelineBean.ErrorStage, errorStageContext)

	p := &Pipeline{
		standaloneRunner:  standaloneRunner,
		pipelineConf:      standaloneRunner.GetPipelineConfig(),
		pipelineBean:      pipelineBean,
		pipes:             pipes,
		errorStageRuntime: errorStageRuntime,
		errorSink:         errorSink,
		offsetTracker:     sourceOffsetTracker,
		MetricRegistry:    metricRegistry,
	}

	p.batchProcessingTimer = util.CreateTimer(metricRegistry, PIPELINE_BATCH_PROCESSING)

	p.batchCountCounter = util.CreateCounter(metricRegistry, PIPELINE_BATCH_COUNT)
	p.batchInputRecordsCounter = util.CreateCounter(metricRegistry, PIPELINE_BATCH_INPUT_RECORDS)
	p.batchOutputRecordsCounter = util.CreateCounter(metricRegistry, PIPELINE_BATCH_OUTPUT_RECORDS)
	p.batchErrorRecordsCounter = util.CreateCounter(metricRegistry, PIPELINE_BATCH_ERROR_RECORDS)
	p.batchErrorMessagesCounter = util.CreateCounter(metricRegistry, PIPELINE_BATCH_ERROR_MESSAGES)

	p.batchCountMeter = util.CreateMeter(metricRegistry, PIPELINE_BATCH_COUNT)
	p.batchInputRecordsMeter = util.CreateMeter(metricRegistry, PIPELINE_BATCH_INPUT_RECORDS)
	p.batchOutputRecordsMeter = util.CreateMeter(metricRegistry, PIPELINE_BATCH_OUTPUT_RECORDS)
	p.batchErrorRecordsMeter = util.CreateMeter(metricRegistry, PIPELINE_BATCH_ERROR_RECORDS)
	p.batchErrorMessagesMeter = util.CreateMeter(metricRegistry, PIPELINE_BATCH_ERROR_MESSAGES)

	p.batchInputRecordsHistogram = util.CreateHistogram5Min(metricRegistry, PIPELINE_INPUT_RECORDS_PER_BATCH)
	p.batchOutputRecordsHistogram = util.CreateHistogram5Min(metricRegistry, PIPELINE_OUTPUT_RECORDS_PER_BATCH)
	p.batchErrorRecordsHistogram = util.CreateHistogram5Min(metricRegistry, PIPELINE_ERROR_RECORDS_PER_BATCH)
	p.batchErrorMessagesHistogram = util.CreateHistogram5Min(metricRegistry, PIPELINE_ERRORS_PER_BATCH)

	return p, nil
}
