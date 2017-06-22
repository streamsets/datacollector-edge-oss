package identity

import (
	"github.com/streamsets/sdc2go/api"
	"github.com/streamsets/sdc2go/container/common"
	"github.com/streamsets/sdc2go/container/execution/runner"
	"github.com/streamsets/sdc2go/stages/stagelibrary"
	"testing"
)

func getStageContext() api.StageContext {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = LIBRARY
	stageConfig.StageName = STAGE_NAME
	stageConfig.Configuration = make([]common.Config, 0)
	return &common.StageContextImpl{
		StageConfig: stageConfig,
		Parameters:  nil,
	}
}

func TestIdentityProcessor(t *testing.T) {
	stageContext := getStageContext()
	stageInstance, err := stagelibrary.CreateStageInstance(LIBRARY, STAGE_NAME)
	if err != nil {
		t.Error(err)
	}
	stageInstance.Init(stageContext)
	records := make([]api.Record, 1)
	records[0] = stageContext.CreateRecord("1", "TestData")
	batch := runner.NewBatchImpl("random", records, "randomOffset")
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{})

	err = stageInstance.(api.Processor).Process(batch, batchMaker)
	if err != nil {
		t.Error("Error in Identity Processor")
	}

	outputRecords := batchMaker.GetStageOutput()
	if len(outputRecords) != 1 {
		t.Error("Excepted 1 records but got - ", len(records))
	}

	if records[0].GetValue() != "TestData" {
		t.Error("Excepted 'TestData' but got - ", records[0].GetValue())
	}

	stageInstance.Destroy()
}
