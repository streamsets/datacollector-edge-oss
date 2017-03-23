package creation

import (
	"github.com/streamsets/dataextractor/container/common"
)

type PipelineBean struct {
	Config               PipelineConfigBean
	Stages               []StageBean
	ErrorStage           StageBean
	StatsAggregatorStage StageBean
}

func NewPipelineBean(pipelineConfig common.PipelineConfiguration) PipelineBean {
	var pipelineBean PipelineBean

	pipelineBean.Config = NewPipelineConfigBean(pipelineConfig)

	stageBeans := make([]StageBean, len(pipelineConfig.Stages))
	for i, stageConfig := range pipelineConfig.Stages {
		stageBeans[i] = NewStageBean(stageConfig)
	}
	pipelineBean.Stages = stageBeans

	if pipelineConfig.ErrorStage.InstanceName != "" {
		pipelineBean.ErrorStage = NewStageBean(pipelineConfig.ErrorStage)
	}

	if pipelineConfig.StatsAggregatorStage.InstanceName != "" {
		pipelineBean.StatsAggregatorStage = NewStageBean(pipelineConfig.StatsAggregatorStage)
	}

	return pipelineBean
}
