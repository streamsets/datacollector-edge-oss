package creation

import (
	"github.com/streamsets/dataextractor/container/common"
)

type StageConfigBean struct {
	StageOnRecordError       string
	StageRequiredFields      []interface{}
	StageRecordPreconditions []interface{}
}

func NewStageConfigBean(pipelineConfig common.StageConfiguration) StageConfigBean {
	stageConfigBean := StageConfigBean{}

	for _, config := range pipelineConfig.Configuration {
		switch config.Name {
		case "stageOnRecordError":
			stageConfigBean.StageOnRecordError = config.Value.(string)
			break
		case "stageRequiredFields":
			stageConfigBean.StageRequiredFields = config.Value.([]interface{})
			break
		case "stageRecordPreconditions":
			stageConfigBean.StageRecordPreconditions = config.Value.([]interface{})
			break
		}
	}
	return stageConfigBean
}
