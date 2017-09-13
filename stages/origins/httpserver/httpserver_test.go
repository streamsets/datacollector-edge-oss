package httpserver

import (
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"testing"
)

func getStageContext(portNumber float64, appId string, parameters map[string]interface{}) *common.StageContextImpl {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = LIBRARY
	stageConfig.StageName = STAGE_NAME
	stageConfig.Configuration = make([]common.Config, 2)
	stageConfig.Configuration[0] = common.Config{
		Name:  "httpConfigs.port",
		Value: portNumber,
	}
	stageConfig.Configuration[1] = common.Config{
		Name:  "httpConfigs.appId",
		Value: appId,
	}
	return &common.StageContextImpl{
		StageConfig: stageConfig,
		Parameters:  parameters,
	}
}

func TestHttpServerOrigin_Init(t *testing.T) {
	portNumber := float64(500)
	appId := "edge"

	stageContext := getStageContext(portNumber, appId, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Error(err)
	}

	stageInstance := stageBean.Stage
	if stageInstance == nil {
		t.Error("Failed to create stage instance")
	}

	if stageInstance.(*HttpServerOrigin).HttpConfigs.Port != portNumber {
		t.Error("Failed to inject config value for port number")
	}

	if stageInstance.(*HttpServerOrigin).HttpConfigs.AppId != appId {
		t.Error("Failed to inject config value for port number")
	}
}
