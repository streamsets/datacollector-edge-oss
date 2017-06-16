package common

import (
	"github.com/streamsets/sdc2go/api"
)

type BaseStage struct {
	stageContext api.StageContext
}

func (b *BaseStage) GetStageContext() api.StageContext {
	return b.stageContext
}

func (b *BaseStage) Init(stageContext api.StageContext) error {
	b.stageContext = stageContext
	return nil
}

func (b *BaseStage) Destroy() error {
	//No OP Destroy
	return nil
}

func (b *BaseStage) GetStageConfig() StageConfiguration {
	return b.stageContext.(*StageContextImpl).StageConfig
}
