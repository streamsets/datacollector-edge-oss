package creation

import (
	"github.com/streamsets/dataextractor/api"
	"github.com/streamsets/dataextractor/container/common"
	"github.com/streamsets/dataextractor/stages/stagelibrary"
)

type StageBean struct {
	Config        common.StageConfiguration
	Stage         api.Stage
	SystemConfigs StageConfigBean
}

func NewStageBean(stageConfig common.StageConfiguration) (StageBean, error) {
	stageInstance, err := stagelibrary.CreateStageInstance(stageConfig.Library, stageConfig.StageName)
	stageBean := StageBean{}
	stageBean.Config = stageConfig
	stageBean.Stage = stageInstance
	stageBean.SystemConfigs = NewStageConfigBean(stageConfig)
	return stageBean, err
}
