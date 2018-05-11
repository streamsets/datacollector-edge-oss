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
package identity

import (
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
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

func (i *IdentityProcessor) Init(stageContext api.StageContext) []validation.Issue {
	return i.BaseStage.Init(stageContext)
}

func (i *IdentityProcessor) Process(batch api.Batch, batchMaker api.BatchMaker) error {
	for _, record := range batch.GetRecords() {
		batchMaker.AddRecord(record)
	}
	return nil
}
