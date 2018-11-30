// +build aws

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
package kinesis

import (
	"fmt"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"testing"
)

func getStageContext(
	stageConfigurationList []common.Config,
	parameters map[string]interface{},
) *common.StageContextImpl {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = Library
	stageConfig.StageName = StageName
	stageConfig.Configuration = stageConfigurationList
	errorSink := common.NewErrorSink()
	return &common.StageContextImpl{
		StageConfig:       &stageConfig,
		Parameters:        parameters,
		ErrorSink:         errorSink,
		ErrorRecordPolicy: common.ErrorRecordPolicyStage,
	}
}

func getTestConfig(
	awsAccessKeyId string,
	awsSecretAccessKey string,
	streamName string,
	preserveOrdering bool,
) []common.Config {
	configuration := []common.Config{
		{
			Name:  "kinesisConfig.awsConfig.awsAccessKeyId",
			Value: awsAccessKeyId,
		},
		{
			Name:  "kinesisConfig.awsConfig.awsSecretAccessKey",
			Value: awsSecretAccessKey,
		},
		{
			Name:  "kinesisConfig.region",
			Value: "US_WEST_2",
		},
		{
			Name:  "kinesisConfig.streamName",
			Value: streamName,
		},
		{
			Name:  "kinesisConfig.preserveOrdering",
			Value: preserveOrdering,
		},
		{
			Name:  "kinesisConfig.dataFormat",
			Value: "JSON",
		},
	}

	return configuration
}

func TestDestination_Init(t *testing.T) {
	config := getTestConfig(
		"awsAccessKeyId",
		"awsSecretAccessKey",
		"sampleStreamName",
		false,
	)
	stageContext := getStageContext(config, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage
	if stageInstance == nil {
		t.Error("Failed to create stage instance")
	}

	if stageInstance.(*Destination).KinesisConfig.Region != "US_WEST_2" {
		t.Error("Failed to inject config value for Region")
	}

	if stageInstance.(*Destination).KinesisConfig.StreamName != "sampleStreamName" {
		t.Error("Failed to inject config value for Stream Name")
	}
}

func TestDestination_Write_PreserveOrdering(t *testing.T) {
	config := getTestConfig(
		"invalidAccessKeyId",
		"invalid",
		"invalid",
		true,
	)
	stageContext := getStageContext(config, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage

	issues := stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Error(issues)
		return
	}

	records := make([]api.Record, 1)
	records[0], _ = stageContext.CreateRecord("1", map[string]interface{}{
		"index": "test data",
	})

	batch := runner.NewBatchImpl("random", records, nil)
	err = stageInstance.(api.Destination).Write(batch)

	if len(stageContext.ErrorSink.GetErrorRecords()[""]) != 1 {
		t.Errorf("Expected 1 error recors with an invalid AWS credential, but got")
		return
	}
}

func TestDestination_Write_NoPreserveOrdering(t *testing.T) {
	config := getTestConfig(
		"invalidAccessKeyId",
		"invalid",
		"invalid",
		false,
	)
	stageContext := getStageContext(config, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage

	issues := stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Error(issues)
		return
	}

	records := make([]api.Record, 1)
	records[0], _ = stageContext.CreateRecord("1", map[string]interface{}{
		"index": "test data",
	})

	batch := runner.NewBatchImpl("random", records, nil)
	err = stageInstance.(api.Destination).Write(batch)

	if len(stageContext.ErrorSink.GetErrorRecords()[""]) != 1 {
		t.Errorf("Expected 1 error recors with an invalid AWS credential, but got")
		return
	}
}

func _TestDestination_WriteUsingTestAccount(t *testing.T) {
	config := getTestConfig(
		"ad",
		"daf",
		"sada",
		false,
	)
	stageContext := getStageContext(config, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage

	issues := stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Error(issues)
		return
	}

	records := make([]api.Record, 10)

	for i := 0; i < 10; i++ {
		records[i], _ = stageContext.CreateRecord("1", map[string]interface{}{
			"index": fmt.Sprintf("test data %d", i),
		})
	}

	batch := runner.NewBatchImpl("random", records, nil)
	err = stageInstance.(api.Destination).Write(batch)

	if stageContext.ErrorSink.GetTotalErrorMessages() != 0 {
		t.Errorf(
			"Expected no stage errors, but encountered error: %s",
			stageContext.ErrorSink.GetStageErrorMessages("")[0].LocalizableMessage,
		)
		return
	}
}
