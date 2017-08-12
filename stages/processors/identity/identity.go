package identity

import (
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
)

const (
	LIBRARY    = "streamsets-datacollector-dev-lib"
	STAGE_NAME = "com_streamsets_pipeline_stage_processor_identity_IdentityProcessor"
	VERSION    = 1
)

type IdentityProcessor struct {
	*common.BaseStage
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &IdentityProcessor{BaseStage: &common.BaseStage{}}
	})
}

func (i *IdentityProcessor) Init(stageContext api.StageContext) error {
	return i.BaseStage.Init(stageContext)
}

func (i *IdentityProcessor) Process(batch api.Batch, batchMaker api.BatchMaker) error {
	for _, record := range batch.GetRecords() {
		batchMaker.AddRecord(i.GetStageContext().CreateRecord("dev-identity", record.GetValue()))
	}
	return nil
}
