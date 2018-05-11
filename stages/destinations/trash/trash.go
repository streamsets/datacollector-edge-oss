// Copyright 2018 StreamSets Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package trash

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
)

const (
	LIBRARY                       = "streamsets-datacollector-basic-lib"
	ERROR_STAGE_NAME              = "com_streamsets_pipeline_stage_destination_devnull_ToErrorNullDTarget"
	NULL_STAGE_NAME               = "com_streamsets_pipeline_stage_destination_devnull_NullDTarget"
	STATS_NULL_STAGE_NAME         = "com_streamsets_pipeline_stage_destination_devnull_StatsNullDTarget"
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
	stagelibrary.SetCreator(LIBRARY, STATS_NULL_STAGE_NAME, func() api.Stage {
		return &TrashDestination{BaseStage: &common.BaseStage{}}
	})
	stagelibrary.SetCreator(LIBRARY, STATS_DPM_DIRECTLY_STAGE_NAME, func() api.Stage {
		return &TrashDestination{BaseStage: &common.BaseStage{}}
	})
}

func (t *TrashDestination) Init(stageContext api.StageContext) []validation.Issue {
	return t.BaseStage.Init(stageContext)
}

func (t *TrashDestination) Write(batch api.Batch) error {
	for _, record := range batch.GetRecords() {
		recordValue, _ := record.Get()
		jsonValue, err := json.Marshal(recordValue.Value)
		if err != nil {
			log.WithError(err).Error("Json Serialization Error")
			t.GetStageContext().ToError(err, record)
		}
		log.WithField("record", string(jsonValue)).Debug("Trashed record")
	}
	return nil
}
