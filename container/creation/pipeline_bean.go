package creation

import (
	"github.com/streamsets/datacollector-edge/container/common"
)

type PipelineBean struct {
	Config               PipelineConfigBean
	Stages               []StageBean
	ErrorStage           StageBean
	StatsAggregatorStage StageBean
}

func NewPipelineBean(
	pipelineConfig common.PipelineConfiguration,
	runtimeParameters map[string]interface{},
) (PipelineBean, error) {
	var pipelineBean PipelineBean
	var err error

	pipelineBean.Config = NewPipelineConfigBean(pipelineConfig)

	stageBeans := make([]StageBean, len(pipelineConfig.Stages))
	for i, stageConfig := range pipelineConfig.Stages {
		stageBeans[i], err = NewStageBean(stageConfig, runtimeParameters)
		if err != nil {
			return pipelineBean, err
		}
	}
	pipelineBean.Stages = stageBeans

	if pipelineConfig.ErrorStage.InstanceName != "" {
		pipelineBean.ErrorStage, err = NewStageBean(pipelineConfig.ErrorStage, runtimeParameters)
		if err != nil {
			return pipelineBean, err
		}
	}

	if pipelineConfig.StatsAggregatorStage.InstanceName != "" {
		pipelineBean.StatsAggregatorStage, err = NewStageBean(pipelineConfig.StatsAggregatorStage, runtimeParameters)
		if err != nil {
			return pipelineBean, err
		}
	}

	return pipelineBean, err
}
