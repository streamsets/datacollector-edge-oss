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
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"testing"
)

func getStageContext() (*common.StageContextImpl, *common.ErrorSink) {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = Library
	stageConfig.StageName = StageName
	stageConfig.Configuration = make([]common.Config, 0)
	errorSink := common.NewErrorSink()
	return &common.StageContextImpl{
		StageConfig:       &stageConfig,
		Parameters:        nil,
		ErrorSink:         errorSink,
		ErrorRecordPolicy: common.ErrorRecordPolicyStage,
	}, errorSink
}

func TestProcessor(t *testing.T) {
	stageContext, errorSink := getStageContext()
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	stageInstance.Init(stageContext)
	records := make([]api.Record, 1)
	records[0], _ = stageContext.CreateRecord("1", "TestData")
	batch := runner.NewBatchImpl("random", records, nil)
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)

	err = stageInstance.(api.Processor).Process(batch, batchMaker)
	if err != nil {
		t.Error("Error in Identity Processor")
	}

	outputRecords := batchMaker.GetStageOutput()

	if len(outputRecords)+int(errorSink.GetTotalErrorRecords()) != 1 {
		t.Error("Excepted 1 records but got - ", len(records))
	}

	stageInstance.Destroy()
}
