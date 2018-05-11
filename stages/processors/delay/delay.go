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
package delay

import (
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"time"
)

const (
	LIBRARY    = "streamsets-datacollector-basic-lib"
	STAGE_NAME = "com_streamsets_pipeline_stage_processor_delay_DelayProcessor"
	VERSION    = 1
)

type DelayProcessor struct {
	*common.BaseStage
	Delay float64 `ConfigDef:"type=NUMBER,required=true"`
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &DelayProcessor{BaseStage: &common.BaseStage{}}
	})
}

func (d *DelayProcessor) Init(stageContext api.StageContext) []validation.Issue {
	return d.BaseStage.Init(stageContext)
}

func (d *DelayProcessor) Process(batch api.Batch, batchMaker api.BatchMaker) error {
	time.Sleep(time.Duration(d.Delay) * time.Millisecond)
	for _, record := range batch.GetRecords() {
		batchMaker.AddRecord(record)
	}
	return nil
}
