package runner

import (
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/validation"
)

type StageRuntime struct {
	pipelineBean creation.PipelineBean
	config       common.StageConfiguration
	stageBean    creation.StageBean
	stageContext api.StageContext
}

func (s *StageRuntime) Init() []validation.Issue {
	var issues []validation.Issue
	s.stageBean.Stage.Init(s.stageContext)
	return issues
}

func (s *StageRuntime) Execute(
	previousOffset string,
	batchSize int,
	batch *BatchImpl,
	batchMaker *BatchMakerImpl,
) (string, error) {
	var newOffset string
	var err error
	if s.stageBean.IsSource() {
		newOffset, err = s.stageBean.Stage.(api.Origin).Produce(previousOffset, batchSize, batchMaker)
	} else if s.stageBean.IsProcessor() {
		err = s.stageBean.Stage.(api.Processor).Process(batch, batchMaker)
	} else if s.stageBean.IsTarget() {
		err = s.stageBean.Stage.(api.Destination).Write(batch)
	}
	return newOffset, err
}

func (s *StageRuntime) Destroy() {
	s.stageBean.Stage.Destroy()
}

func NewStageRuntime(
	pipelineBean creation.PipelineBean,
	stageBean creation.StageBean,
	stageContext api.StageContext,
) StageRuntime {
	return StageRuntime{
		pipelineBean: pipelineBean,
		config:       stageBean.Config,
		stageBean:    stageBean,
		stageContext: stageContext,
	}
}
