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
package dev_random

import (
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"math/rand"
	"strings"
	"time"
)

const (
	LIBRARY                  = "streamsets-datacollector-dev-lib"
	STAGE_NAME               = "com_streamsets_pipeline_stage_devtest_RandomSource"
	ConfFields               = "fields"
	ConfDelay                = "delay"
	ConfMaxRecordsToGenerate = "maxRecordsToGenerate"
)

var randomOffset = "random"

type DevRandom struct {
	*common.BaseStage
	Fields               string  `ConfigDef:"type=STRING,required=true"`
	Delay                float64 `ConfigDef:"type=NUMBER,required=true"`
	MaxRecordsToGenerate float64 `ConfigDef:"type=NUMBER,required=true"`
	fieldsList           []string
	recordsProduced      float64
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &DevRandom{BaseStage: &common.BaseStage{}}
	})
}

func (d *DevRandom) Init(stageContext api.StageContext) []validation.Issue {
	issues := d.BaseStage.Init(stageContext)
	d.fieldsList = strings.Split(d.Fields, ",")
	d.recordsProduced = 0
	return issues
}

func (d *DevRandom) Produce(lastSourceOffset *string, maxBatchSize int, batchMaker api.BatchMaker) (*string, error) {
	r := rand.New(rand.NewSource(99))
	time.Sleep(time.Duration(d.Delay) * time.Millisecond)
	for i := 0; i < maxBatchSize && d.recordsProduced < d.MaxRecordsToGenerate; i++ {
		var recordValue = make(map[string]interface{})
		for _, field := range d.fieldsList {
			recordValue[field] = r.Int63()
		}
		recordId := common.CreateRecordId("dev-random", i)
		if record, err := d.GetStageContext().CreateRecord(recordId, recordValue); err == nil {
			batchMaker.AddRecord(record)
		} else {
			d.GetStageContext().ToError(err, record)
		}
		d.recordsProduced++
	}
	return &randomOffset, nil
}
