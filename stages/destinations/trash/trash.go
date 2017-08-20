package trash

import (
	"encoding/json"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"log"
)

const (
	LIBRARY                       = "streamsets-datacollector-basic-lib"
	ERROR_STAGE_NAME              = "com_streamsets_pipeline_stage_destination_devnull_ToErrorNullDTarget"
	NULL_STAGE_NAME               = "com_streamsets_pipeline_stage_destination_devnull_NullDTarget"
	STATS_DPM_DIRECTLY_STAGE_NAME = "com_streamsets_pipeline_stage_destination_devnull_StatsDpmDirectlyDTarget"
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
	stagelibrary.SetCreator(LIBRARY, STATS_DPM_DIRECTLY_STAGE_NAME, func() api.Stage {
		return &TrashDestination{BaseStage: &common.BaseStage{}}
	})
}

func (t *TrashDestination) Init(stageContext api.StageContext) error {
	return t.BaseStage.Init(stageContext)
}

func (t *TrashDestination) Write(batch api.Batch) error {
	for _, record := range batch.GetRecords() {
		jsonValue, err := json.Marshal(record.Get().Value)
		if err != nil {
			log.Println("[Error] Json Serialization Error", err)
			t.GetStageContext().ToError(err, record)
		}
		log.Println("[DEBUG] Trash record: ", string(jsonValue))
	}
	return nil
}
