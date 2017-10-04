package runner

import (
	"github.com/streamsets/datacollector-edge/api"
)

type BatchMakerImpl struct {
	stagePipe        StagePipe
	stageOutput      map[string][]api.Record
	singleOutputLane string
}

func (b *BatchMakerImpl) GetLanes() []string {
	return b.stagePipe.OutputLanes
}

func (b *BatchMakerImpl) AddRecord(record api.Record, outputLanes ...string) {
	if len(outputLanes) > 0 {
		for _, outputLane := range outputLanes {
			b.stageOutput[outputLane] = append(b.stageOutput[outputLane], record)
		}
	} else {
		b.stageOutput[b.singleOutputLane] = append(b.stageOutput[b.singleOutputLane], record)
	}
}

func (b *BatchMakerImpl) GetStageOutput(outputLane ...string) []api.Record {
	if len(outputLane) > 0 {
		return b.stageOutput[outputLane[0]]
	}
	return b.stageOutput[b.singleOutputLane]
}

func NewBatchMakerImpl(stagePipe StagePipe) *BatchMakerImpl {
	batchMaker := &BatchMakerImpl{stagePipe: stagePipe}
	batchMaker.stageOutput = make(map[string][]api.Record)
	for _, outputLane := range stagePipe.OutputLanes {
		batchMaker.stageOutput[outputLane] = make([]api.Record, 0)
	}
	if len(stagePipe.OutputLanes) > 0 {
		batchMaker.singleOutputLane = stagePipe.OutputLanes[0]
	}
	return batchMaker
}
