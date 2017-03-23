package runner

import (
	"fmt"
	"github.com/streamsets/dataextractor/api"
	"github.com/streamsets/dataextractor/container/common"
	"github.com/streamsets/dataextractor/container/creation"
	"github.com/streamsets/dataextractor/container/validation"
)

type StageRuntime struct {
	pipelineBean creation.PipelineBean
	config       common.StageConfiguration
	stageBean    creation.StageBean
}

func (s *StageRuntime) Init() []validation.Issue {
	var issues []validation.Issue
	fmt.Println("StageRuntime Init")
	s.stageBean.Stage.Init(s.config)
	return issues
}

func (s *StageRuntime) Execute(
	previousOffset string,
	batchSize int,
	batch *BatchImpl,
	batchMaker *BatchMakerImpl,
) {
	if len(s.config.OutputLanes) > 0 {
		s.stageBean.Stage.(api.Origin).Produce(previousOffset, batchSize, batchMaker)
	} else {
		s.stageBean.Stage.(api.Destination).Write(batch)
	}
}

func NewStageRuntime(pipelineBean creation.PipelineBean, stageBean creation.StageBean) StageRuntime {
	return StageRuntime{
		pipelineBean: pipelineBean,
		config:       stageBean.Config,
		stageBean:    stageBean,
	}
}
