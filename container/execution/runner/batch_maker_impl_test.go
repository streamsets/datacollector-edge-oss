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
package runner

import (
	"github.com/streamsets/datacollector-edge/container/common"
	"testing"
)

func TestMultipleLanesClone(t *testing.T) {
	batchMaker := NewBatchMakerImpl(StagePipe{}, false)

	stageConfig := common.StageConfiguration{}
	stageConfig.Library = "abc"
	stageConfig.StageName = "bcd"
	stageContext := &common.StageContextImpl{
		StageConfig: &stageConfig,
		Parameters:  nil,
	}

	record, _ := stageContext.CreateRecord("abc", map[string]interface{}{
		"a": 1,
		"b": 2,
	})

	batchMaker.AddRecord(record, "output1", "output2")

	output1LaneRecords := batchMaker.GetStageOutput("output1")
	output2LaneRecords := batchMaker.GetStageOutput("output2")

	if len(output1LaneRecords) != 1 {
		t.Errorf("Expected 1 record(s), actual : %d", len(output1LaneRecords))
	}

	if len(output2LaneRecords) != 1 {
		t.Errorf("Expected 1 record(s), actual : %d", len(output2LaneRecords))
	}

	// Basically the record interface contains a pointer to RecordImpl
	output1LaneRecord := output1LaneRecords[0]
	output2LaneRecord := output2LaneRecords[0]

	if output1LaneRecord == record {
		t.Errorf("Output1 lane does not have the record cloned")
	}

	if output2LaneRecord == record {
		t.Errorf("Output2 lane does not have the record cloned")
	}

}
