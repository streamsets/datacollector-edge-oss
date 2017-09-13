package identity

import (
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"testing"
)

func getStageContext() *common.StageContextImpl {
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
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	stageInstance.Init(stageContext)
	records := make([]api.Record, 1)
	records[0], _ = stageContext.CreateRecord("1", "TestData")
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

	if records[0].Get().Value != "TestData" {
		t.Error("Excepted 'TestData' but got - ", records[0].Get().Value)
	}

	stageInstance.Destroy()
}
