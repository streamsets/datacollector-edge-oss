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
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/execution"
)

type PipeBatch interface {
	GetBatchSize() int
	GetPreviousOffset() *string
	SetNewOffset(offset *string)
	GetBatch(pipe StagePipe) *BatchImpl
	StartStage(pipe StagePipe) *BatchMakerImpl
	CompleteStage(batchMaker *BatchMakerImpl)
	GetErrorSink() *common.ErrorSink
	GetEventSink() *common.EventSink
	GetInputRecords() int64
	GetOutputRecords() int64
	GetEventRecords() int64
	GetErrorRecords() int64
	GetErrorMessages() int64
	OverrideStageOutput(pipe Pipe, stageOutput *execution.StageOutput)
	GetSnapshotsOfAllStagesOutput() []execution.StageOutput
}

type FullPipeBatch struct {
	offsetTracker       execution.SourceOffsetTracker
	batchSize           int
	fullPayload         map[string][]api.Record
	inputRecords        int64
	outputRecords       int64
	eventRecords        int64
	errorSink           *common.ErrorSink
	eventSink           *common.EventSink
	StageOutputSnapshot []execution.StageOutput
}

func (b *FullPipeBatch) GetBatchSize() int {
	return b.batchSize
}

func (b *FullPipeBatch) GetPreviousOffset() *string {
	return b.offsetTracker.GetOffset()
}

func (b *FullPipeBatch) SetNewOffset(newOffset *string) {
	b.offsetTracker.SetOffset(newOffset)
}

func (b *FullPipeBatch) GetBatch(pipe StagePipe) *BatchImpl {
	records := make([]api.Record, 0)
	for _, inputLane := range pipe.InputLanes {
		if len(b.fullPayload[inputLane]) > 0 {
			for _, record := range b.fullPayload[inputLane] {
				records = append(records, record)
			}
		}
	}

	if pipe.IsTarget() && b.fullPayload != nil {
		b.outputRecords += int64(len(records))
	}
	return NewBatchImpl(pipe.Stage.config.InstanceName, records, b.offsetTracker.GetOffset())
}

func (b *FullPipeBatch) StartStage(pipe StagePipe) *BatchMakerImpl {
	return NewBatchMakerImpl(pipe, b.StageOutputSnapshot != nil)
}

func (b *FullPipeBatch) CompleteStage(batchMaker *BatchMakerImpl) {
	stageInstanceName := batchMaker.stagePipe.Stage.config.InstanceName
	if batchMaker.stagePipe.IsSource() {
		b.inputRecords += batchMaker.GetSize() +
			int64(len(b.errorSink.GetStageErrorRecords(stageInstanceName)))
	}
	if !batchMaker.stagePipe.IsTarget() {
		outputLanes := batchMaker.stagePipe.Stage.config.OutputLanes
		for _, outputLane := range outputLanes {
			b.fullPayload[outputLane] = batchMaker.GetStageOutput(outputLane)
		}
	}

	if len(batchMaker.stagePipe.EventLanes) > 0 {
		b.fullPayload[batchMaker.stagePipe.EventLanes[0]] = b.eventSink.GetStageEvents(stageInstanceName)
	}

	if b.StageOutputSnapshot != nil {
		stageOutput := execution.StageOutput{
			InstanceName: stageInstanceName,
			Output:       batchMaker.StageOutputSnapshot,
			ErrorRecords: b.errorSink.GetStageErrorRecords(stageInstanceName),
			StageErrors:  b.errorSink.GetStageErrorMessages(stageInstanceName),
			EventRecords: b.eventSink.GetStageEvents(stageInstanceName),
		}
		b.StageOutputSnapshot = append(b.StageOutputSnapshot, stageOutput)
	}

	if batchMaker.stagePipe.IsTarget() {
		b.outputRecords -= int64(len(b.errorSink.GetStageErrorRecords(stageInstanceName)))
	}

	b.eventRecords += int64(len(b.eventSink.GetStageEvents(stageInstanceName)))
}

func (b *FullPipeBatch) GetErrorSink() *common.ErrorSink {
	return b.errorSink
}

func (b *FullPipeBatch) GetEventSink() *common.EventSink {
	return b.eventSink
}

func (b *FullPipeBatch) GetInputRecords() int64 {
	return b.inputRecords
}

func (b *FullPipeBatch) GetOutputRecords() int64 {
	return b.outputRecords
}

func (b *FullPipeBatch) GetEventRecords() int64 {
	return b.eventRecords
}

func (b *FullPipeBatch) GetErrorRecords() int64 {
	return b.errorSink.GetTotalErrorRecords()
}

func (b *FullPipeBatch) GetErrorMessages() int64 {
	return b.errorSink.GetTotalErrorMessages()
}

func (b *FullPipeBatch) OverrideStageOutput(pipe Pipe, stageOutput *execution.StageOutput) {
	b.fullPayload = make(map[string][]api.Record)
	for _, outputLane := range pipe.GetOutputLanes() {
		b.fullPayload[outputLane] = stageOutput.Output[outputLane]
	}
	if len(pipe.GetEventLanes()) > 0 {
		b.fullPayload[pipe.GetEventLanes()[0]] = stageOutput.EventRecords
	}

	if b.StageOutputSnapshot != nil {
		stageOutput := execution.StageOutput{
			InstanceName: stageOutput.InstanceName,
			Output:       stageOutput.Output,
			EventRecords: stageOutput.EventRecords,
			ErrorRecords: stageOutput.ErrorRecords,
			StageErrors:  stageOutput.StageErrors,
		}
		b.StageOutputSnapshot = append(b.StageOutputSnapshot, stageOutput)
	}
}

func (b *FullPipeBatch) GetSnapshotsOfAllStagesOutput() []execution.StageOutput {
	return b.StageOutputSnapshot
}

func NewFullPipeBatch(
	tracker execution.SourceOffsetTracker,
	batchSize int,
	errorSink *common.ErrorSink,
	eventSink *common.EventSink,
	snapshotStagesOutput bool,
) PipeBatch {
	fullPipeBatch := &FullPipeBatch{
		offsetTracker: tracker,
		batchSize:     batchSize,
		errorSink:     errorSink,
		eventSink:     eventSink,
	}

	fullPipeBatch.fullPayload = make(map[string][]api.Record)

	if snapshotStagesOutput {
		fullPipeBatch.StageOutputSnapshot = make([]execution.StageOutput, 0)
	}

	return fullPipeBatch
}
