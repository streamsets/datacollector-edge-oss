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
package creation

import (
	"github.com/streamsets/datacollector-edge/container/common"
)

const (
	ExecutionMode        = "executionMode"
	DeliveryGuarantee    = "deliveryGuarantee"
	ShouldRetry          = "shouldRetry"
	RetryAttempts        = "retryAttempts"
	MemoryLimit          = "memoryLimit"
	MemoryLmtExceeded    = "memoryLimitExceeded"
	NotifyOnStates       = "notifyOnStates"
	EmailIds             = "emailIDs"
	Constants            = "constants"
	BadRecordsHandling   = "badRecordsHandling"
	TestOriginStage      = "testOriginStage"
	RateLimit            = "rateLimit"
	MaxRunners           = "maxRunners"
	StatsAggregatorStage = "statsAggregatorStage"
	ErrorRecordPolicy    = "errorRecordPolicy"

	ClusterSlaveMemory   = "clusterSlaveMemory"
	ClusterSlaveJavaOpts = "clusterSlaveJavaOpts"
	ClusterLauncherEnv   = "clusterLauncherEnv"
	MesosDispatchUrl     = "mesosDispatcherURL"
	HdfsS3ConfigDir      = "hdfsS3ConfDir"
	WebHookConfigs       = "webhookConfigs"
)

type PipelineConfigBean struct {
	Version              string
	ExecutionMode        string
	DeliveryGuarantee    string
	ShouldRetry          bool
	RetryAttempts        float64
	MemoryLimit          string
	MemoryLimitExceeded  string
	NotifyOnStates       []interface{}
	EmailIDs             []interface{}
	Constants            map[string]interface{}
	BadRecordsHandling   string
	TestOriginStage      string
	ErrorRecordPolicy    string
	StatsAggregatorStage string
	RateLimit            float64
	MaxRunners           float64
}

func NewPipelineConfigBean(pipelineConfig common.PipelineConfiguration) PipelineConfigBean {
	pipelineConfigBean := PipelineConfigBean{}

	for _, config := range pipelineConfig.Configuration {
		if config.Value == nil {
			continue
		}
		switch config.Name {
		case ExecutionMode:
			pipelineConfigBean.ExecutionMode = config.Value.(string)
		case DeliveryGuarantee:
			pipelineConfigBean.DeliveryGuarantee = config.Value.(string)
		case ShouldRetry:
			pipelineConfigBean.ShouldRetry = config.Value.(bool)
		case RetryAttempts:
			pipelineConfigBean.RetryAttempts = config.Value.(float64)
		case MemoryLimit:
			pipelineConfigBean.MemoryLimit = config.Value.(string)
		case MemoryLmtExceeded:
			pipelineConfigBean.MemoryLimitExceeded = config.Value.(string)
		case NotifyOnStates:
			pipelineConfigBean.NotifyOnStates = config.Value.([]interface{})
		case EmailIds:
			pipelineConfigBean.EmailIDs = config.Value.([]interface{})
		case Constants:
			constants := config.Value.([]interface{})
			pipelineConfigBean.Constants = make(map[string]interface{})
			for _, constant := range constants {
				constantMap := constant.(map[string]interface{})
				key := constantMap["key"]
				pipelineConfigBean.Constants[key.(string)] = constantMap["value"]
			}
		case ErrorRecordPolicy:
			pipelineConfigBean.ErrorRecordPolicy = config.Value.(string)
		case BadRecordsHandling:
			pipelineConfigBean.BadRecordsHandling = config.Value.(string)
		case TestOriginStage:
			pipelineConfigBean.TestOriginStage = config.Value.(string)
		case StatsAggregatorStage:
			pipelineConfigBean.StatsAggregatorStage = config.Value.(string)
		case RateLimit:
			pipelineConfigBean.RateLimit = config.Value.(float64)
		case MaxRunners:
			pipelineConfigBean.MaxRunners = config.Value.(float64)
		}
	}

	return pipelineConfigBean
}

func GetDefaultPipelineConfigs() []common.Config {
	pipelineConfigs := []common.Config{
		{Name: ExecutionMode, Value: "STANDALONE"},
		{Name: DeliveryGuarantee, Value: "AT_LEAST_ONCE"},
		{Name: ShouldRetry, Value: true},
		{Name: RetryAttempts, Value: -1},
		{Name: MemoryLimit, Value: "${jvm:maxMemoryMB() * 0.65}"},
		{Name: MemoryLmtExceeded, Value: "STOP_PIPELINE"},
		{Name: NotifyOnStates, Value: []string{common.RUN_ERROR, common.STOPPED, common.FINISHED}},
		{Name: EmailIds, Value: []string{}},
		{Name: Constants, Value: []string{}},
		{Name: BadRecordsHandling, Value: "streamsets-datacollector-basic-lib::com_streamsets_pipeline_stage_destination_devnull_ToErrorNullDTarget::1"},
		{Name: ErrorRecordPolicy, Value: common.ErrorRecordPolicyOriginal},
		{Name: ClusterSlaveMemory, Value: 1024},
		{Name: ClusterSlaveJavaOpts, Value: "-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -Dlog4j.debug"},
		{Name: ClusterLauncherEnv, Value: []string{}},
		{Name: MesosDispatchUrl, Value: nil},
		{Name: HdfsS3ConfigDir, Value: nil},
		{Name: RateLimit, Value: 0},
		{Name: MaxRunners, Value: 0},
		{Name: WebHookConfigs, Value: []interface{}{}},
		{Name: StatsAggregatorStage, Value: "streamsets-datacollector-basic-lib::com_streamsets_pipeline_stage_destination_devnull_StatsDpmDirectlyDTarget::1"},
	}

	return pipelineConfigs
}

func GetTrashErrorStageInstance() *common.StageConfiguration {
	return &common.StageConfiguration{
		InstanceName:  "Discard_ErrorStage",
		Library:       "streamsets-datacollector-basic-lib",
		StageName:     "com_streamsets_pipeline_stage_destination_devnull_ToErrorNullDTarget",
		StageVersion:  "1",
		Configuration: []common.Config{},
		UiInfo:        map[string]interface{}{},
		InputLanes:    []string{},
		OutputLanes:   []string{},
		EventLanes:    []string{},
	}
}

func GetDefaultStatsAggregatorStageInstance() *common.StageConfiguration {
	return &common.StageConfiguration{
		InstanceName:  "WritetoDPMdirectly_StatsAggregatorStage",
		Library:       "streamsets-datacollector-basic-lib",
		StageName:     "com_streamsets_pipeline_stage_destination_devnull_StatsDpmDirectlyDTarget",
		StageVersion:  "1",
		Configuration: []common.Config{},
		UiInfo:        map[string]interface{}{},
		InputLanes:    []string{},
		OutputLanes:   []string{},
		EventLanes:    []string{},
	}
}
