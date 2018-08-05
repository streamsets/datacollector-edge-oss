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
	"github.com/streamsets/datacollector-edge/container/execution"
	"github.com/streamsets/datacollector-edge/container/util"
	"time"
)

const (
	InputRecords    = ".inputRecords"
	OutputRecords   = ".outputRecords"
	ErrorRecords    = ".errorRecords"
	StageErrors     = ".stageErrors"
	BatchProcessing = ".batchProcessing"
)

type Pipe interface {
	Init() []validation.Issue
	Process(pipeBatch PipeBatch) error
	Destroy()
	IsSource() bool
	IsProcessor() bool
	IsTarget() bool
	GetInstanceName() string
	GetStageContext() api.StageContext
	GetOutputLanes() []string
	GetEventLanes() []string
}

type StagePipe struct {
	config                      execution.Config
	Stage                       StageRuntime
	InputLanes                  []string
	OutputLanes                 []string
	EventLanes                  []string
	inputRecordsCounter         metrics.Counter
	outputRecordsCounter        metrics.Counter
	errorRecordsCounter         metrics.Counter
	stageErrorsCounter          metrics.Counter
	inputRecordsMeter           metrics.Meter
	outputRecordsMeter          metrics.Meter
	errorRecordsMeter           metrics.Meter
	stageErrorsMeter            metrics.Meter
	inputRecordsHistogram       metrics.Histogram
	outputRecordsHistogram      metrics.Histogram
	errorRecordsHistogram       metrics.Histogram
	stageErrorsHistogram        metrics.Histogram
	processingTimer             metrics.Timer
	outputRecordsPerLaneCounter map[string]metrics.Counter
	outputRecordsPerLaneMeter   map[string]metrics.Meter
}

func (s *StagePipe) Init() []validation.Issue {
	issues := s.Stage.Init()
	if len(issues) == 0 {
		metricRegistry := s.Stage.stageContext.GetMetrics()
		metricsKey := "stage." + s.Stage.config.InstanceName

		s.inputRecordsCounter = util.CreateCounter(metricRegistry, metricsKey+InputRecords)
		s.outputRecordsCounter = util.CreateCounter(metricRegistry, metricsKey+OutputRecords)
		s.errorRecordsCounter = util.CreateCounter(metricRegistry, metricsKey+ErrorRecords)
		s.stageErrorsCounter = util.CreateCounter(metricRegistry, metricsKey+StageErrors)

		s.inputRecordsMeter = util.CreateMeter(metricRegistry, metricsKey+InputRecords)
		s.outputRecordsMeter = util.CreateMeter(metricRegistry, metricsKey+OutputRecords)
		s.errorRecordsMeter = util.CreateMeter(metricRegistry, metricsKey+ErrorRecords)
		s.stageErrorsMeter = util.CreateMeter(metricRegistry, metricsKey+StageErrors)

		s.inputRecordsHistogram = util.CreateHistogram5Min(metricRegistry, metricsKey+InputRecords)
		s.outputRecordsHistogram = util.CreateHistogram5Min(metricRegistry, metricsKey+OutputRecords)
		s.errorRecordsHistogram = util.CreateHistogram5Min(metricRegistry, metricsKey+ErrorRecords)
		s.stageErrorsHistogram = util.CreateHistogram5Min(metricRegistry, metricsKey+StageErrors)

		s.processingTimer = util.CreateTimer(metricRegistry, metricsKey+BatchProcessing)

		outputAndEventLanes := s.Stage.config.GetOutputAndEventLanes()
		if len(outputAndEventLanes) > 0 {
			s.outputRecordsPerLaneCounter = make(map[string]metrics.Counter)
			s.outputRecordsPerLaneMeter = make(map[string]metrics.Meter)
			for _, lane := range outputAndEventLanes {
				s.outputRecordsPerLaneCounter[lane] =
					util.CreateCounter(metricRegistry, metricsKey+":"+lane+OutputRecords)
				s.outputRecordsPerLaneMeter[lane] =
					util.CreateMeter(metricRegistry, metricsKey+":"+lane+OutputRecords)
			}
		}
	}

	return issues
}

func (s *StagePipe) Process(pipeBatch PipeBatch) error {
	log.WithField("stage", s.Stage.config.InstanceName).Debug("Processing Stage")
	start := time.Now()
	batchMaker := pipeBatch.StartStage(*s)
	batchImpl := pipeBatch.GetBatch(*s)
	newOffset, err := s.Stage.Execute(pipeBatch.GetPreviousOffset(), pipeBatch.GetBatchSize(), batchImpl, batchMaker)

	if err != nil {
		return err
	}

	if s.IsSource() {
		pipeBatch.SetNewOffset(newOffset)
	}
	pipeBatch.CompleteStage(batchMaker)

	// Update metric registry
	s.processingTimer.UpdateSince(start)

	instanceName := s.Stage.config.InstanceName
	errorSink := pipeBatch.GetErrorSink()
	eventSink := pipeBatch.GetEventSink()

	stageErrorRecordsCount := int64(len(errorSink.GetStageErrorRecords(instanceName)))
	stageErrorMessagesCount := int64(len(errorSink.GetStageErrorMessages(instanceName)))

	inputRecordsCount := int64(len(batchImpl.records))
	outputRecordsCount := batchMaker.GetSize()

	if s.IsTarget() {
		// Assumption is that the target will not drop any record.
		// Records are sent to destination or to the error sink.
		outputRecordsCount = inputRecordsCount - stageErrorRecordsCount
	}

	s.inputRecordsCounter.Inc(inputRecordsCount)
	s.inputRecordsMeter.Mark(inputRecordsCount)
	s.inputRecordsHistogram.Update(inputRecordsCount)

	s.outputRecordsCounter.Inc(outputRecordsCount)
	s.outputRecordsMeter.Mark(outputRecordsCount)
	s.outputRecordsHistogram.Update(outputRecordsCount)

	s.errorRecordsCounter.Inc(stageErrorRecordsCount)
	s.errorRecordsMeter.Mark(stageErrorRecordsCount)
	s.errorRecordsHistogram.Update(stageErrorRecordsCount)
	s.stageErrorsCounter.Inc(stageErrorMessagesCount)
	s.stageErrorsMeter.Mark(stageErrorMessagesCount)
	s.stageErrorsHistogram.Update(stageErrorMessagesCount)

	if len(s.Stage.config.OutputLanes) > 0 {
		for _, lane := range s.Stage.config.OutputLanes {
			laneCount := int64(len(batchMaker.GetStageOutput(lane)))
			s.outputRecordsPerLaneCounter[lane].Inc(laneCount)
			s.outputRecordsPerLaneMeter[lane].Mark(laneCount)
		}
	}

	if len(s.Stage.config.EventLanes) > 0 {
		eventLane := s.Stage.config.EventLanes[0]
		laneCount := int64(len(eventSink.GetStageEvents(instanceName)))
		s.outputRecordsPerLaneCounter[eventLane].Inc(laneCount)
		s.outputRecordsPerLaneMeter[eventLane].Mark(laneCount)
	}

	return nil
}

func (s *StagePipe) Destroy() {
	s.Stage.Destroy()
}

func (s *StagePipe) IsSource() bool {
	return s.Stage.stageBean.IsSource()
}

func (s *StagePipe) IsProcessor() bool {
	return s.Stage.stageBean.IsProcessor()
}

func (s *StagePipe) IsTarget() bool {
	return s.Stage.stageBean.IsTarget()
}

func (s *StagePipe) GetInstanceName() string {
	return s.Stage.config.InstanceName
}

func (s *StagePipe) GetStageContext() api.StageContext {
	return s.Stage.stageContext
}

func (s *StagePipe) GetOutputLanes() []string {
	return s.OutputLanes
}

func (s *StagePipe) GetEventLanes() []string {
	return s.EventLanes
}

func NewStagePipe(stage StageRuntime, config execution.Config) Pipe {
	stagePipe := &StagePipe{}
	stagePipe.config = config
	stagePipe.Stage = stage
	stagePipe.InputLanes = stage.config.InputLanes
	stagePipe.OutputLanes = stage.config.OutputLanes
	stagePipe.EventLanes = stage.config.EventLanes
	return stagePipe
}
