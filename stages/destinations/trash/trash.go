package trash

import (
	"context"
	"encoding/json"
	"github.com/streamsets/dataextractor/api"
	"github.com/streamsets/dataextractor/stages/stagelibrary"
	"log"
)

const (
	LIBRARY          = "streamsets-datacollector-basic-lib"
	ERROR_STAGE_NAME = "com_streamsets_pipeline_stage_destination_devnull_ToErrorNullDTarget"
	NULL_STAGE_NAME  = "com_streamsets_pipeline_stage_destination_devnull_NullDTarget"
)

type TrashDestination struct {
}

func init() {
	stagelibrary.SetCreator(LIBRARY, ERROR_STAGE_NAME, func() api.Stage {
		return &TrashDestination{}
	})
	stagelibrary.SetCreator(LIBRARY, NULL_STAGE_NAME, func() api.Stage {
		return &TrashDestination{}
	})
}

func (t *TrashDestination) Init(ctx context.Context) error {
	return nil
}

func (t *TrashDestination) Destroy() error {
	return nil
}

func (t *TrashDestination) Write(batch api.Batch) error {
	for _, record := range batch.GetRecords() {
		jsonValue, err := json.Marshal(record.Value)
		if err != nil {
			panic(err)
		}

		log.Println("[DEBUG] Trash record: ", string(jsonValue))
	}
	return nil
}
