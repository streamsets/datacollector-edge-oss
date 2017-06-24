package creation

import (
	"github.com/streamsets/sdc2go/container/common"
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
	StatsAggregatorStage string
	RateLimit            float64
	MaxRunners           float64
}

func NewPipelineConfigBean(pipelineConfig common.PipelineConfiguration) PipelineConfigBean {
	pipelineConfigBean := PipelineConfigBean{}

	for _, config := range pipelineConfig.Configuration {
		switch config.Name {
		case "executionMode":
			pipelineConfigBean.ExecutionMode = config.Value.(string)
			break
		case "deliveryGuarantee":
			pipelineConfigBean.DeliveryGuarantee = config.Value.(string)
			break
		case "shouldRetry":
			pipelineConfigBean.ShouldRetry = config.Value.(bool)
			break
		case "retryAttempts":
			pipelineConfigBean.RetryAttempts = config.Value.(float64)
			break
		case "memoryLimit":
			pipelineConfigBean.MemoryLimit = config.Value.(string)
			break
		case "memoryLimitExceeded":
			pipelineConfigBean.MemoryLimitExceeded = config.Value.(string)
			break
		case "notifyOnStates":
			pipelineConfigBean.NotifyOnStates = config.Value.([]interface{})
			break
		case "emailIDs":
			pipelineConfigBean.EmailIDs = config.Value.([]interface{})
			break
		case "constants":
			constants := config.Value.([]interface{})
			pipelineConfigBean.Constants = make(map[string]interface{})
			for _, constant := range constants {
				constantMap := constant.(map[string]interface{})
				key := constantMap["key"]
				pipelineConfigBean.Constants[key.(string)] = constantMap["value"]
			}
			break
		case "badRecordsHandling":
			pipelineConfigBean.BadRecordsHandling = config.Value.(string)
			break
		case "statsAggregatorStage":
			pipelineConfigBean.StatsAggregatorStage = config.Value.(string)
			break
		case "rateLimit":
			pipelineConfigBean.RateLimit = config.Value.(float64)
			break
		case "maxRunners":
			pipelineConfigBean.MaxRunners = config.Value.(float64)
			break
		}
	}

	return pipelineConfigBean
}

func GetDefaultPipelineConfigs() []common.Config {
	pipelineConfigs := []common.Config{
		common.Config{Name: "executionMode", Value: "STANDALONE"},
		common.Config{Name: "deliveryGuarantee", Value: "AT_LEAST_ONCE"},
		common.Config{Name: "shouldRetry", Value: true},
		common.Config{Name: "retryAttempts", Value: -1},
		common.Config{Name: "memoryLimit", Value: "${jvm:maxMemoryMB() * 0.65}"},
		common.Config{Name: "memoryLimitExceeded", Value: "STOP_PIPELINE"},
		common.Config{Name: "notifyOnStates", Value: []string{"RUN_ERROR", "STOPPED", "FINISHED"}},
		common.Config{Name: "emailIDs", Value: []string{}},
		common.Config{Name: "constants", Value: []string{}},
		common.Config{Name: "badRecordsHandling", Value: "streamsets-datacollector-basic-lib::com_streamsets_pipeline_stage_destination_devnull_ToErrorNullDTarget::1"},
		common.Config{Name: "clusterSlaveMemory", Value: 1024},
		common.Config{Name: "clusterSlaveJavaOpts", Value: "-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -Dlog4j.debug"},
		common.Config{Name: "clusterLauncherEnv", Value: []string{}},
		common.Config{Name: "mesosDispatcherURL", Value: nil},
		common.Config{Name: "hdfsS3ConfDir", Value: nil},
		common.Config{Name: "rateLimit", Value: 0},
		common.Config{Name: "maxRunners", Value: 0},
		common.Config{Name: "webhookConfigs", Value: []interface{}{}},
		common.Config{Name: "statsAggregatorStage", Value: "streamsets-datacollector-basic-lib::com_streamsets_pipeline_stage_destination_devnull_StatsDpmDirectlyDTarget::1"},
	}

	return pipelineConfigs
}

func GetTrashErrorStageInstance() common.StageConfiguration {
	return common.StageConfiguration{
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

func GetDefaultStatsAggregatorStageInstance() common.StageConfiguration {
	return common.StageConfiguration{
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
