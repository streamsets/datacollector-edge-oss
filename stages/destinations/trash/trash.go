package trash

import (
	"encoding/json"
	"github.com/streamsets/dataextractor/api"
	"github.com/streamsets/dataextractor/container/common"
	"github.com/streamsets/dataextractor/stages/stagelibrary"
	"log"
)

const (
	LIBRARY          = "streamsets-datacollector-basic-lib"
	ERROR_STAGE_NAME = "com_streamsets_pipeline_stage_destination_devnull_ToErrorNullDTarget"
	NULL_STAGE_NAME  = "com_streamsets_pipeline_stage_destination_devnull_NullDTarget"
)

type TrashDestination struct {
	*common.BaseStage
}

func init() {
	stagelibrary.SetCreator(LIBRARY, ERROR_STAGE_NAME, func() api.Stage {
		return &TrashDestination{BaseStage: &common.BaseStage{}}
	})
	stagelibrary.SetCreator(LIBRARY, NULL_STAGE_NAME, func() api.Stage {
		return &TrashDestination{BaseStage: &common.BaseStage{}}
	})
}

func (t *TrashDestination) Init(stageContext api.StageContext) error {
	return t.BaseStage.Init(stageContext)
}

func (t *TrashDestination) Write(batch api.Batch) error {
	for _, record := range batch.GetRecords() {
		jsonValue, err := json.Marshal(record.GetValue())
		if err != nil {
			panic(err)
		}

		log.Println("[DEBUG] Trash record: ", string(jsonValue))
	}
	return nil
}
