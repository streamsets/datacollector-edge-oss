// +build 386 windows,amd64 windows

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

package windows

import (
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"testing"
)

func createStageContext(logName string) *common.StageContextImpl {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = Library
	stageConfig.StageName = StageName
	stageConfig.Configuration = make([]common.Config, 2)

	stageConfig.Configuration[0] = common.Config{
		Name:  "logName",
		Value: logName,
	}
	stageConfig.Configuration[1] = common.Config{
		Name:  "readMode",
		Value: "ALL",
	}
	return &common.StageContextImpl{
		StageConfig: &stageConfig,
		Parameters:  nil,
	}
}

func testWindowsEventLogRead(t *testing.T, logName string, maxBatchSize int) {
	stageContext := createStageContext(logName)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage

	issues := stageInstance.Init(stageContext)

	if len(issues) > 0 {
		t.Fatalf("Error when Initializing stage %s", issues[0].Message)
	}

	defer stageInstance.Destroy()
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)

	_, err = stageInstance.(api.Origin).Produce(nil, maxBatchSize, batchMaker)

	if err != nil {
		t.Fatalf("Error when Producing %s", err.Error())
	}

	records := batchMaker.GetStageOutput()

	if len(records) <= 0 {
		t.Fatalf("Did not read any records")
	} else {
		for _, event := range records {
			rootField, _ := event.Get()
			rootFieldValue := rootField.Value.(map[string](*api.Field))
			actualLogName := rootFieldValue["LogName"].Value
			if actualLogName != logName {
				t.Fatalf("Wrong Log Name. Expected : %s, Actual : %s", logName, actualLogName)
			}
		}
	}
}

func TestWindowsApplicationLogRead(t *testing.T) {
	testWindowsEventLogRead(t, Application, 1)
}

func TestWindowsSystemLogRead(t *testing.T) {
	testWindowsEventLogRead(t, System, 1)
}
