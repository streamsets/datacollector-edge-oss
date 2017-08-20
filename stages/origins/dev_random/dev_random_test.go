package dev_random

import (
	"fmt"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/fieldtype"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"strings"
	"testing"
)

func getStageContext(fields string, delay float64) api.StageContext {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = LIBRARY
	stageConfig.StageName = STAGE_NAME
	stageConfig.Configuration = make([]common.Config, 2)
	stageConfig.Configuration[0] = common.Config{
		Name:  CONF_FIELDS,
		Value: fields,
	}
	stageConfig.Configuration[1] = common.Config{
		Name:  CONF_DELAY,
		Value: delay,
	}
	return &common.StageContextImpl{
		StageConfig: stageConfig,
		Parameters:  nil,
	}
}

func TestDevRandomOrigin(t *testing.T) {
	fields := "a,b,c"
	stageContext := getStageContext(fields, 10)
	stageInstance, err := stagelibrary.CreateStageInstance(LIBRARY, STAGE_NAME)
	if err != nil {
		t.Error(err)
	}
	err = stageInstance.Init(stageContext)
	if err != nil {
		t.Error(err)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{})
	_, err = stageInstance.(api.Origin).Produce("", 5, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	records := batchMaker.GetStageOutput()
	if len(records) != 5 {
		t.Error("Excepted 5 records but got - ", len(records))
	}

	for _, record := range records {
		rootField := record.Get()
		if rootField.Type != fieldtype.MAP {
			t.Error("Exception Map field type but got - ", rootField.Type, " Value: ", rootField.Value)
			return
		}

		rootFieldValue := rootField.Value.(map[string]api.Field)
		for key, field := range rootFieldValue {
			if field.Type != fieldtype.INTEGER {
				t.Error("Exception Map field type but got - ", field.Type, " Value: ", field.Value)
				return
			}
			fmt.Println("key - ", key)
			if !strings.Contains(fields, key) {
				t.Error("Invalid key")
			}
		}
	}
	stageInstance.Destroy()
}
