package creation

import (
	"github.com/streamsets/sdc2go/api"
	"github.com/streamsets/sdc2go/container/common"
	"github.com/streamsets/sdc2go/stages/stagelibrary"
)

const (
	STAGE_TYPE = "stageType"
	SOURCE     = "SOURCE"
	PROCESSOR  = "PROCESSOR"
	TARGET     = "TARGET"
)

type StageBean struct {
	Config        common.StageConfiguration
	Stage         api.Stage
	SystemConfigs StageConfigBean
}

func (s *StageBean) IsSource() bool {
	uiInfo := s.Config.UiInfo
	return uiInfo[STAGE_TYPE] == SOURCE
}

func (s *StageBean) IsProcessor() bool {
	uiInfo := s.Config.UiInfo
	return uiInfo[STAGE_TYPE] == PROCESSOR
}

func (s *StageBean) IsTarget() bool {
	uiInfo := s.Config.UiInfo
	return uiInfo[STAGE_TYPE] == TARGET
}

func NewStageBean(stageConfig common.StageConfiguration) (StageBean, error) {
	stageInstance, err := stagelibrary.CreateStageInstance(stageConfig.Library, stageConfig.StageName)
	stageBean := StageBean{}
	stageBean.Config = stageConfig
	stageBean.Stage = stageInstance
	stageBean.SystemConfigs = NewStageConfigBean(stageConfig)
	return stageBean, err
}
