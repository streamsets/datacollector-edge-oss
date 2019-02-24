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
package s3

import (
	"errors"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/dataformats"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	_ "github.com/streamsets/datacollector-edge/stages/services"
	"testing"
)

type mockFileHelper struct {
	receivedData map[string][]api.Record
	errorCase    bool
}

func (f *mockFileHelper) Handle(
	records []api.Record,
	bucket string,
	keyPrefix string,
) (*s3manager.UploadOutput, error) {
	f.receivedData[bucket+":"+keyPrefix] = records

	if f.errorCase {
		return nil, errors.New("sample error")
	}

	return nil, nil
}

func getStageContext(
	stageConfigurationList []common.Config,
	parameters map[string]interface{},
) (*common.StageContextImpl, *common.ErrorSink) {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = Library
	stageConfig.StageName = StageName
	stageConfig.Configuration = stageConfigurationList
	errorSink := common.NewErrorSink()

	serviceConfig := &common.ServiceConfiguration{}
	serviceConfig.Service = dataformats.DataFormatGeneratorServiceName
	serviceConfig.Configuration = []common.Config{
		{
			Name:  "dataFormat",
			Value: "JSON",
		},
	}
	stageConfig.Services = []*common.ServiceConfiguration{serviceConfig}

	return &common.StageContextImpl{
		StageConfig:       &stageConfig,
		Parameters:        parameters,
		ErrorSink:         errorSink,
		ErrorRecordPolicy: common.ErrorRecordPolicyStage,
	}, errorSink
}

func getTestConfig(
	awsAccessKeyId string,
	awsSecretAccessKey string,
	bucketTemplate string,
	partitionTemplate string,
) []common.Config {
	configuration := []common.Config{
		{
			Name:  "s3TargetConfigBean.s3Config.awsConfig.awsAccessKeyId",
			Value: awsAccessKeyId,
		},
		{
			Name:  "s3TargetConfigBean.s3Config.awsConfig.awsSecretAccessKey",
			Value: awsSecretAccessKey,
		},
		{
			Name:  "s3TargetConfigBean.s3Config.region",
			Value: "US_WEST_2",
		},
		{
			Name:  "s3TargetConfigBean.s3Config.bucketTemplate",
			Value: bucketTemplate,
		},
		{
			Name:  "s3TargetConfigBean.s3Config.endpoint",
			Value: "",
		},
		{
			Name:  "s3TargetConfigBean.s3Config.commonPrefix",
			Value: "mockFilePrefix",
		},
		{
			Name:  "s3TargetConfigBean.s3Config.delimiter",
			Value: "/",
		},
		{
			Name:  "s3TargetConfigBean.sseConfig.useSSE",
			Value: false,
		},
		{
			Name:  "s3TargetConfigBean.sseConfig.encryption",
			Value: "S3",
		},
		{
			Name:  "s3TargetConfigBean.sseConfig.kmsKeyId",
			Value: "",
		},
		{
			Name:  "s3TargetConfigBean.sseConfig.encryptionContext",
			Value: nil,
		},
		{
			Name:  "s3TargetConfigBean.sseConfig.customerKey",
			Value: "",
		},
		{
			Name:  "s3TargetConfigBean.sseConfig.customerKeyMd5",
			Value: "",
		},
		{
			Name:  "s3TargetConfigBean.proxyConfig.connectionTimeout",
			Value: 10,
		},
		{
			Name:  "s3TargetConfigBean.proxyConfig.socketTimeout",
			Value: 50,
		},
		{
			Name:  "s3TargetConfigBean.proxyConfig.retryCount",
			Value: 3,
		},
		{
			Name:  "s3TargetConfigBean.proxyConfig.useProxy",
			Value: false,
		},
		{
			Name:  "s3TargetConfigBean.proxyConfig.proxyHost",
			Value: "",
		},
		{
			Name:  "s3TargetConfigBean.proxyConfig.proxyPort",
			Value: 0,
		},
		{
			Name:  "s3TargetConfigBean.proxyConfig.proxyUser",
			Value: "",
		},
		{
			Name:  "s3TargetConfigBean.proxyConfig.proxyPassword",
			Value: "",
		},
		{
			Name:  "s3TargetConfigBean.tmConfig.threadPoolSize",
			Value: 10,
		},
		{
			Name:  "s3TargetConfigBean.tmConfig.multipartUploadThreshold",
			Value: 268435456,
		},
		{
			Name:  "s3TargetConfigBean.tmConfig.minimumUploadPartSize",
			Value: 5242880,
		},
		{
			Name:  "s3TargetConfigBean.partitionTemplate",
			Value: partitionTemplate,
		},
		{
			Name:  "s3TargetConfigBean.timeZoneID",
			Value: "UTC",
		},
		{
			Name:  "s3TargetConfigBean.timeDriverTemplate",
			Value: "adas",
		},
		{
			Name:  "s3TargetConfigBean.fileNamePrefix",
			Value: "sdc",
		},
		{
			Name:  "s3TargetConfigBean.fileNameSuffix",
			Value: "json",
		},
		{
			Name:  "s3TargetConfigBean.compress",
			Value: false,
		},
	}
	return configuration
}

func TestDestination_Init(t *testing.T) {
	config := getTestConfig(
		"awsAccessKeyId",
		"awsSecretAccessKey",
		"sampleS3Bucket",
		"",
	)
	stageContext, _ := getStageContext(config, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage
	if stageInstance == nil {
		t.Error("Failed to create stage instance")
	}

	serviceInstance := stageBean.Services[0].Service
	stageContext.Services = map[string]api.Service{
		dataformats.DataFormatGeneratorServiceName: serviceInstance,
	}
	// initialize service instance
	issues := serviceInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}

	if stageInstance.(*Destination).S3TargetConfigBean.S3Config.Region != "US_WEST_2" {
		t.Error("Failed to inject config value for Region")
	}

	if stageInstance.(*Destination).S3TargetConfigBean.S3Config.BucketTemplate != "sampleS3Bucket" {
		t.Error("Failed to inject config value for S3 Bucket template")
	}

	issues = stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Error(issues)
		return
	}
}

func TestDestination_Write(t *testing.T) {
	config := getTestConfig(
		"awsAccessKeyId",
		"awsSecretAccessKey",
		"sampleS3Bucket",
		"",
	)
	stageContext, errorSink := getStageContext(config, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage.(*Destination)

	serviceInstance := stageBean.Services[0].Service
	stageContext.Services = map[string]api.Service{
		dataformats.DataFormatGeneratorServiceName: serviceInstance,
	}
	// initialize service instance
	issues := serviceInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}

	stageInstance.Init(stageContext)
	mockHelper := &mockFileHelper{receivedData: make(map[string][]api.Record)}
	stageInstance.fileHelper = mockHelper

	records := make([]api.Record, 2)
	records[0], _ = stageContext.CreateRecord(
		"abc",
		map[string]interface{}{
			"a": float64(2.55),
			"b": float64(3.55),
			"c": "random",
		},
	)
	records[1], _ = stageContext.CreateRecord(
		"abc",
		map[string]interface{}{
			"a": float64(2.55),
			"b": float64(3.55),
			"c": "random",
		},
	)
	batch := runner.NewBatchImpl("toError", records, nil)
	err = stageInstance.Write(batch)
	if err != nil {
		t.Error(err)
		return
	}

	// validate write method
	if len(mockHelper.receivedData) != 1 {
		t.Error("Failed to write data to S3")
	}

	if records, ok := mockHelper.receivedData["sampleS3Bucket:mockFilePrefixsdc-"]; ok != true {
		t.Error("Invalid S3 bucket and file name passed to FileHelper handle method")
	} else if len(records) != 2 {
		t.Error("Invalid records passed to FileHelper handle method")
	}

	if errorSink.GetTotalErrorRecords() != 0 {
		t.Fatal("expected 0 records in error sink")
	}
}

func TestDestination_Write_withPartition(t *testing.T) {
	config := getTestConfig(
		"awsAccessKeyId",
		"awsSecretAccessKey",
		"sampleS3Bucket",
		"${record:value('/partitionField')}",
	)
	stageContext, errorSink := getStageContext(config, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage.(*Destination)

	serviceInstance := stageBean.Services[0].Service
	stageContext.Services = map[string]api.Service{
		dataformats.DataFormatGeneratorServiceName: serviceInstance,
	}
	// initialize service instance
	issues := serviceInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}

	stageInstance.Init(stageContext)
	mockHelper := &mockFileHelper{receivedData: make(map[string][]api.Record)}
	stageInstance.fileHelper = mockHelper

	records := make([]api.Record, 2)
	records[0], _ = stageContext.CreateRecord(
		"abc",
		map[string]interface{}{
			"a":              float64(2.55),
			"b":              float64(3.55),
			"partitionField": "p1",
		},
	)
	records[1], _ = stageContext.CreateRecord(
		"abc",
		map[string]interface{}{
			"a":              float64(2.55),
			"b":              float64(3.55),
			"partitionField": "p2",
		},
	)
	batch := runner.NewBatchImpl("toError", records, nil)
	err = stageInstance.Write(batch)
	if err != nil {
		t.Error(err)
		return
	}

	// validate write method
	if len(mockHelper.receivedData) != 2 {
		t.Error("Failed to write data to S3 with partition")
	}

	if records, ok := mockHelper.receivedData["sampleS3Bucket:mockFilePrefixp1/sdc-"]; ok != true {
		t.Error("Invalid S3 bucket and file name passed to FileHelper handle method")
	} else if len(records) != 1 {
		t.Error("Invalid records passed to FileHelper handle method")
	}

	if records, ok := mockHelper.receivedData["sampleS3Bucket:mockFilePrefixp2/sdc-"]; ok != true {
		t.Error("Invalid S3 bucket and file name passed to FileHelper handle method")
	} else if len(records) != 1 {
		t.Error("Invalid records passed to FileHelper handle method")
	}

	if errorSink.GetTotalErrorRecords() != 0 {
		t.Fatal("expected 0 records in error sink")
	}
}

func TestDestination_Write_ErrorCase(t *testing.T) {
	config := getTestConfig(
		"awsAccessKeyId",
		"awsSecretAccessKey",
		"sampleS3Bucket",
		"${record:value('/partitionField')}",
	)
	stageContext, errorSink := getStageContext(config, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage.(*Destination)

	serviceInstance := stageBean.Services[0].Service
	stageContext.Services = map[string]api.Service{
		dataformats.DataFormatGeneratorServiceName: serviceInstance,
	}
	// initialize service instance
	issues := serviceInstance.Init(stageContext)
	if len(issues) != 0 {
		t.Error(issues[0].Message)
	}

	stageInstance.Init(stageContext)
	mockHelper := &mockFileHelper{receivedData: make(map[string][]api.Record), errorCase: true}
	stageInstance.fileHelper = mockHelper

	records := make([]api.Record, 2)
	records[0], _ = stageContext.CreateRecord(
		"abc",
		map[string]interface{}{
			"a":              float64(2.55),
			"b":              float64(3.55),
			"partitionField": "p1",
		},
	)
	records[1], _ = stageContext.CreateRecord(
		"abc",
		map[string]interface{}{
			"a":              float64(2.55),
			"b":              float64(3.55),
			"partitionField": "p2",
		},
	)
	batch := runner.NewBatchImpl("toError", records, nil)
	err = stageInstance.Write(batch)
	if err != nil {
		t.Error(err)
		return
	}

	// validate write method
	if errorSink.GetTotalErrorRecords() != 2 {
		t.Fatal("expected 2 records in error sink")
	}
}
