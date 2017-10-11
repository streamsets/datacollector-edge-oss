package runner

import (
	"github.com/streamsets/datacollector-edge/container/common"
	"testing"
)

func TestMultipleLanesClone(t *testing.T) {
	batchMaker := NewBatchMakerImpl(StagePipe{})

	stageConfig := common.StageConfiguration{}
	stageConfig.Library = "abc"
	stageConfig.StageName = "bcd"
	stageContext := &common.StageContextImpl{
		StageConfig: stageConfig,
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

	//Basically the record interface contains a pointer to RecordImpl
	output1LaneRecord := output1LaneRecords[0]
	output2LaneRecord := output2LaneRecords[0]

	if output1LaneRecord == record {
		t.Errorf("Output1 lane does not have the record cloned")
	}

	if output2LaneRecord == record {
		t.Errorf("Output2 lane does not have the record cloned")
	}

}
