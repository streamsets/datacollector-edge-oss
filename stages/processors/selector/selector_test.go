package selector

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
	stageConfig.Configuration = []common.Config{
		{
			Name: "lanePredicates",
			Value: []map[string]interface{}{
				{
					"outputLane": "lane1",
					"predicate":  "${record:value('/a') == NULL}",
				},
				{
					"outputLane": "lane2",
					"predicate":  "default",
				},
			},
		},
	}
	stageConfig.OutputLanes = []string{
		"lane1",
		"lane2",
	}
	return &common.StageContextImpl{
		StageConfig: stageConfig,
		Parameters:  nil,
	}
}

func TestHttpServerOrigin_Init(t *testing.T) {
	stageContext := getStageContext()
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Error(err)
	}

	stageInstance := stageBean.Stage.(*SelectorProcessor)
	if stageInstance == nil {
		t.Error("Failed to create stage instance")
	}

	if stageInstance.LanePredicates == nil {
		t.Error("Failed to inject config value for lane predicate")
		return
	}

	if len(stageInstance.LanePredicates) != 2 {
		t.Error("Failed to inject config value for lane predicate")
	}

	if stageInstance.LanePredicates[0]["predicate"] != "${record:value('/a') == NULL}" {
		t.Error("Failed to inject config value for lane predicate")
	}

	if stageInstance.LanePredicates[1]["predicate"] != "default" {
		t.Error("Failed to inject config value for lane predicate")
	}

}

func TestSelectorProcessor(t *testing.T) {
	stageContext := getStageContext()
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	err = stageInstance.Init(stageContext)
	if err != nil {
		t.Error(err)
		return
	}
	records := make([]api.Record, 1)
	records[0], _ = stageContext.CreateRecord("1", map[string]interface{}{"a": "sample"})
	batch := runner.NewBatchImpl("random", records, "randomOffset")
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{})

	err = stageInstance.(api.Processor).Process(batch, batchMaker)
	if err != nil {
		t.Error("Error in Identity Processor")
	}

	outputRecords := batchMaker.GetStageOutput("lane2")
	if len(outputRecords) != 1 {
		t.Error("Excepted 1 records but got - ", len(outputRecords))
		return
	}

	recordValue, err := outputRecords[0].Get("/a")
	if recordValue.Value != "sample" {
		t.Error("Excepted 'sample' but got - ", recordValue.Value)
	}

	stageInstance.Destroy()
}
