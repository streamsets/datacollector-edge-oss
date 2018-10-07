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
package toerror

import (
	"errors"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
)

const (
	Library   = "streamsets-datacollector-basic-lib"
	StageName = "com_streamsets_pipeline_stage_destination_toerror_ToErrorDTarget"
)

type Destination struct {
	*common.BaseStage
}

func init() {
	stagelibrary.SetCreator(Library, StageName, func() api.Stage {
		return &Destination{BaseStage: &common.BaseStage{}}
	})
}

func (d *Destination) Init(stageContext api.StageContext) []validation.Issue {
	return d.BaseStage.Init(stageContext)
}

func (d *Destination) Write(batch api.Batch) error {
	for _, record := range batch.GetRecords() {
		d.GetStageContext().ToError(errors.New("error target"), record)
	}
	return nil
}
