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
	"errors"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"math/rand"
)

const (
	Library   = "streamsets-datacollector-dev-lib"
	StageName = "com_streamsets_pipeline_stage_devtest_RandomErrorProcessor"
)

var randomError = errors.New("random error")

type Processor struct {
	*common.BaseStage
}

func init() {
	stagelibrary.SetCreator(Library, StageName, func() api.Stage {
		return &Processor{BaseStage: &common.BaseStage{}}
	})
}

func (p *Processor) Init(stageContext api.StageContext) []validation.Issue {
	return p.BaseStage.Init(stageContext)
}

func (p *Processor) Process(batch api.Batch, batchMaker api.BatchMaker) error {
	for _, record := range batch.GetRecords() {
		if rand.Float32() < 0.5 {
			batchMaker.AddRecord(record)
		} else {
			p.GetStageContext().ToError(randomError, record)
		}
	}

	if rand.Float32() < 0.5 {
		p.GetStageContext().ReportError(randomError)
	}
	return nil
}
