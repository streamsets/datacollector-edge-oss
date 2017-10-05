package runner

import (
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
)

type PipeBatch interface {
	GetBatchSize() int
	GetPreviousOffset() map[string]string
	SetNewOffset(offset map[string]string)
	GetBatch(pipe Pipe) BatchImpl
	SkipStage(pipe Pipe)
	StartStage(pipe StagePipe) BatchMakerImpl
	CompleteStage(maker api.BatchMaker)
	CompleteStagePipe(stage StagePipe)
	GetLaneOutputRecords(pipelineLanes []string) map[string][]api.Record
	OverrideStageOutput(pipe StagePipe, stageOutput StageOutput)
	GetSnapshotsOfAllStagesOutput() []StageOutput
	GetErrorSink() *common.ErrorSink
	GetEventSink() EventSink
	MoveLane(inputLane string, outputLane string)
	MoveLaneCopying(inputLane string, outputLanes []string)
	CombineLanes(lanes []string, to string)
	GetInputRecords() int64
	GetOutputRecords() int64
	GetErrorRecords() int64
	GetErrorMessages() int64
}

type FullPipeBatch struct {
	offsetTracker SourceOffsetTracker
	batchSize     int
	fullPayload   map[string][]api.Record
	inputRecords  int64
	outputRecords int64
	errorSink     *common.ErrorSink
}

func (b *FullPipeBatch) GetBatchSize() int {
	return b.batchSize
}

func (b *FullPipeBatch) GetPreviousOffset() string {
	return b.offsetTracker.GetOffset()
}

func (b *FullPipeBatch) SetNewOffset(newOffset string) {
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
	return NewBatchMakerImpl(pipe)
}

func (b *FullPipeBatch) CompleteStage(batchMaker *BatchMakerImpl) {
	if batchMaker.stagePipe.IsSource() {
		b.inputRecords += batchMaker.GetSize() +
			int64(len(b.errorSink.GetStageErrorRecords(batchMaker.stagePipe.Stage.config.InstanceName)))
	}
	if !batchMaker.stagePipe.IsTarget() {
		outputLanes := batchMaker.stagePipe.Stage.config.OutputLanes
		b.fullPayload = make(map[string][]api.Record)
		for _, outputLane := range outputLanes {
			b.fullPayload[outputLane] = batchMaker.GetStageOutput(outputLane)
		}
	}
}

func (b *FullPipeBatch) GetErrorSink() *common.ErrorSink {
	return b.errorSink
}

func (b *FullPipeBatch) GetInputRecords() int64 {
	return b.inputRecords
}

func (b *FullPipeBatch) GetOutputRecords() int64 {
	return b.outputRecords
}

func (b *FullPipeBatch) GetErrorRecords() int64 {
	return b.errorSink.GetTotalErrorRecords()
}

func (b *FullPipeBatch) GetErrorMessages() int64 {
	return b.errorSink.GetTotalErrorMessages()
}

func NewFullPipeBatch(tracker SourceOffsetTracker, batchSize int, errorSink *common.ErrorSink) *FullPipeBatch {
	return &FullPipeBatch{
		offsetTracker: tracker,
		batchSize:     batchSize,
		errorSink:     errorSink,
	}
}
