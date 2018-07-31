// +build tensorflow

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
package tensorflow

import (
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"testing"
)

func getStageContext(
	configuration []common.Config,
	parameters map[string]interface{},
) *common.StageContextImpl {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = Library
	stageConfig.StageName = StageName
	stageConfig.Configuration = configuration
	errorSink := common.NewErrorSink()
	return &common.StageContextImpl{
		StageConfig:       &stageConfig,
		Parameters:        parameters,
		ErrorSink:         errorSink,
		ErrorRecordPolicy: common.ErrorRecordPolicyOriginal,
	}
}

func TestProcessor_Init(t *testing.T) {
	stageContext := getStageContext(GetIrisModelConfig(), nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage
	if stageInstance == nil {
		t.Error("Failed to create stage instance")
	}

	if stageInstance.(*Processor).Conf.ModelPath != "test_data/iris_saved_model" {
		t.Error("Failed to inject config value for modelPath")
	}

	if len(stageInstance.(*Processor).Conf.ModelTags) != 1 && stageInstance.(*Processor).Conf.ModelTags[0] != "serve" {
		t.Error("Failed to inject config value for modelPath")
	}

	issues := stageInstance.Init(stageContext)

	if len(issues) > 0 {
		t.Error(issues)
	}
}

func TestProcessor_Process(t *testing.T) {
	stageContext := getStageContext(GetIrisModelConfig(), nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage
	issues := stageInstance.Init(stageContext)

	if len(issues) > 0 {
		t.Error(issues)
	}

	records := make([]api.Record, 1)
	testData := map[string]interface{}{
		"petalLength": float32(6.2),
		"petalWidth":  float32(2.8),
		"sepalLength": float32(5.6),
		"sepalWidth":  float32(2.2),
	}
	records[0], _ = stageContext.CreateRecord("1", testData)
	batch := runner.NewBatchImpl("random", records, nil)
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)

	err = stageInstance.(api.Processor).Process(batch, batchMaker)
	if err != nil {
		t.Error("Error in TensorFlow Processor procss method", err)
	}

	err = stageInstance.(api.Processor).Process(batch, batchMaker)
	if err != nil {
		t.Error("Error in TensorFlow Processor procss method", err)
	}

	err = stageInstance.Destroy()
	if err != nil {
		t.Error(err)
	}
}

func GetIrisModelConfig() []common.Config {
	petalLength := make(map[string]interface{})
	petalLength["operation"] = "PetalLength"
	petalLength["index"] = float64(0)
	petalLength["fields"] = []string{"/petalLength"}
	petalLength["shape"] = []float64{1}
	petalLength["tensorDataType"] = "FLOAT"

	petalWidth := make(map[string]interface{})
	petalWidth["operation"] = "PetalWidth"
	petalWidth["index"] = float64(0)
	petalWidth["fields"] = []string{"/petalWidth"}
	petalWidth["shape"] = []float64{1}
	petalWidth["tensorDataType"] = "FLOAT"

	sepalLength := make(map[string]interface{})
	sepalLength["operation"] = "SepalLength"
	sepalLength["index"] = float64(0)
	sepalLength["fields"] = []string{"/sepalLength"}
	sepalLength["shape"] = []float64{1}
	sepalLength["tensorDataType"] = "FLOAT"

	sepalWidth := make(map[string]interface{})
	sepalWidth["operation"] = "SepalWidth"
	sepalWidth["index"] = float64(0)
	sepalWidth["fields"] = []string{"/sepalWidth"}
	sepalWidth["shape"] = []float64{1}
	sepalWidth["tensorDataType"] = "FLOAT"

	inputConfigs := []interface{}{
		petalLength,
		petalWidth,
		sepalLength,
		sepalWidth,
	}

	outputConfig1 := make(map[string]interface{})
	outputConfig1["operation"] = "dnn/head/predictions/ExpandDims"
	outputConfig1["index"] = float64(0)
	outputConfig1["tensorDataType"] = "FLOAT"

	outputConfig2 := make(map[string]interface{})
	outputConfig2["operation"] = "dnn/head/predictions/probabilities"
	outputConfig2["index"] = float64(0)
	outputConfig2["tensorDataType"] = "FLOAT"

	outputConfigs := []interface{}{
		outputConfig1,
		outputConfig2,
	}

	return []common.Config{
		{
			Name:  "conf.modelPath",
			Value: "test_data/iris_saved_model",
		},
		{
			Name:  "conf.modelTags",
			Value: []string{"serve"},
		},
		{
			Name:  "conf.inputConfigs",
			Value: inputConfigs,
		},
		{
			Name:  "conf.outputConfigs",
			Value: outputConfigs,
		},
		{
			Name:  "conf.useEntireBatch",
			Value: false,
		},
		{
			Name:  "conf.outputField",
			Value: "/output",
		},
	}
}
