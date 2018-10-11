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
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"testing"
)

func getStageContext() (*common.StageContextImpl, *common.ErrorSink, *common.EventSink) {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = Library
	stageConfig.StageName = StageName
	errorSink := common.NewErrorSink()
	eventSink := common.NewEventSink()
	return &common.StageContextImpl{
		StageConfig:       &stageConfig,
		ErrorSink:         errorSink,
		ErrorRecordPolicy: common.ErrorRecordPolicyStage,
		EventSink:         eventSink,
	}, errorSink, eventSink
}

func TestDestination(t *testing.T) {
	stageContext, errorSink, _ := getStageContext()
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage.(*Destination)
	if stageInstance == nil {
		t.Error("Failed to create stage instance")
	}

	stageInstance.Init(stageContext)

	records := make([]api.Record, 2)
	records[0], _ = stageContext.CreateRecord(
		"abc",
		map[string]interface{}{
			"a": float64(2.55),
			"b": float64(3.55),
			"c": "random",
		},
	)
	records[1], _ = stageContext.CreateRecord(
		"abc",
		map[string]interface{}{
			"a": float64(2.55),
			"b": float64(3.55),
			"c": "random",
		},
	)
	batch := runner.NewBatchImpl("toError", records, nil)
	err = stageInstance.Write(batch)
	if err != nil {
		t.Fatal("Error when writing: " + err.Error())
	}

	if errorSink.GetTotalErrorRecords() != 2 {
		t.Fatal("Failed to write records to error sink")
	}

	stageInstance.Destroy()
}
