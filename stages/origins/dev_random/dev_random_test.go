package dev_random

import (
	"fmt"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/fieldtype"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"strings"
	"testing"
)

func getStageContext(fields string, delay float64, parameters map[string]interface{}) *common.StageContextImpl {
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
		Parameters:  parameters,
	}
}

func TestDevRandom_Init(t *testing.T) {
	fields := "a,b,c"
	stageContext := getStageContext(fields, 10, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage

	if stageInstance.(*DevRandom).Fields != "a,b,c" {
		t.Error("Failed to inject config value for Fields")
	}

	if stageInstance.(*DevRandom).Delay != 10 {
		t.Error("Failed to inject config value for Delay")
	}
}

func TestDevRandomOrigin(t *testing.T) {
	fields := "a,b,c"
	stageContext := getStageContext(fields, 10, nil)

	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage

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
		rootField, _ := record.Get()
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

func TestDevRandom_Init_Parameter(t *testing.T) {
	fields := "${fields}"
	stageContext := getStageContext(fields, 10, nil)
	_, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err == nil || !strings.Contains(err.Error(), "No parameter 'fields' found") {
		t.Error("Excepted error - No parameter 'fields' found")
	}
}

func TestDevRandom_Init_StringEL(t *testing.T) {
	fields := "${str:trim(FIELDS_PARAM)}"
	parameters := map[string]interface{}{
		"FIELDS_PARAM": "x,y,z  ",
	}
	stageContext := getStageContext(fields, 10, parameters)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	err = stageInstance.Init(stageContext)
	if err != nil {
		t.Error(err)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{})
	_, err = stageInstance.(api.Origin).Produce("", 1, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	records := batchMaker.GetStageOutput()
	if len(records) != 1 {
		t.Error("Excepted 5 records but got - ", len(records))
	}

	for _, record := range records {
		rootField, _ := record.Get()
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
			if !((key == "x") || (key == "y") || (key == "z")) {
				t.Error("Invalid key")
			}
		}
	}
	stageInstance.Destroy()
}
