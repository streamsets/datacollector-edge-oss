package runner

import (
	"github.com/streamsets/dataextractor/api"
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
	GetErrorSink() ErrorSink
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
	fullPayload   []api.Record
	inputRecords  int64
	outputRecords int64
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
	if pipe.IsTarget() && b.fullPayload != nil {
		b.outputRecords += int64(len(b.fullPayload))
	}
	return NewBatchImpl(pipe.Stage.config.InstanceName, b.fullPayload, b.offsetTracker.GetOffset())
}

func (b *FullPipeBatch) StartStage(pipe StagePipe) *BatchMakerImpl {
	return NewBatchMakerImpl(pipe)
}

func (b *FullPipeBatch) CompleteStage(batchMaker *BatchMakerImpl) {
	if batchMaker.stagePipe.IsSource() {
		b.inputRecords += int64(len(batchMaker.GetStageOutput()))
	}
	b.fullPayload = batchMaker.GetStageOutput()
}

func (b *FullPipeBatch) GetInputRecords() int64 {
	return b.inputRecords
}

func (b *FullPipeBatch) GetOutputRecords() int64 {
	return b.outputRecords
}

func (b *FullPipeBatch) GetErrorRecords() int64 {
	return 0
}

func (b *FullPipeBatch) GetErrorMessages() int64 {
	return 0
}

func NewFullPipeBatch(tracker SourceOffsetTracker, batchSize int) *FullPipeBatch {
	return &FullPipeBatch{
		offsetTracker: tracker,
		batchSize:     batchSize,
	}
}
