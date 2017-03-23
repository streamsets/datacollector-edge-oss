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
	GetInputRecords() int
	GetOutputRecords() int
	GetErrorRecords() int
	GetErrorMessages() int
}

type FullPipeBatch struct {
	offsetTracker SourceOffsetTracker
	batchSize     int
	fullPayload   []api.Record
}

func (b *FullPipeBatch) GetBatchSize() int {
	return b.batchSize
}

func (b *FullPipeBatch) GetPreviousOffset() map[string]string {
	return b.offsetTracker.GetOffsets()
}

func (b *FullPipeBatch) SetNewOffset(map[string]string) {
	b.offsetTracker.CommitOffset("", "")
}

func (b *FullPipeBatch) GetBatch(pipe StagePipe) *BatchImpl {
	return NewBatchImpl(pipe.Stage.config.InstanceName, b.fullPayload, "sourceOffset")
}

func (b *FullPipeBatch) StartStage(pipe StagePipe) *BatchMakerImpl {
	return NewBatchMakerImpl(pipe)
}

func (b *FullPipeBatch) CompleteStage(batchMaker *BatchMakerImpl) {
	b.fullPayload = batchMaker.getStageOutput()
}

func NewFullPipeBatch(tracker SourceOffsetTracker, batchSize int) *FullPipeBatch {
	return &FullPipeBatch{
		offsetTracker: tracker,
		batchSize:     batchSize,
	}
}
