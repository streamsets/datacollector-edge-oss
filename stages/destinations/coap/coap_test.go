package coap

import (
	"context"
	"github.com/streamsets/dataextractor/api"
	"github.com/streamsets/dataextractor/container/common"
	"github.com/streamsets/dataextractor/container/execution/runner"
	"github.com/streamsets/dataextractor/stages/stagelibrary"
	"testing"
)

func getContext(resourceUrl string, coapMethod string, messageType string) context.Context {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = LIBRARY
	stageConfig.StageName = STAGE_NAME
	stageConfig.Configuration = make([]common.Config, 3)
	stageConfig.Configuration[0] = common.Config{
		Name:  CONF_RESOURCE_URL,
		Value: resourceUrl,
	}
	stageConfig.Configuration[1] = common.Config{
		Name:  CONF_COAP_METHOD,
		Value: coapMethod,
	}
	stageConfig.Configuration[2] = common.Config{
		Name:  CONF_RESOURCE_TYPE,
		Value: messageType,
	}

	stageContext := common.StageContext{
		StageConfig: stageConfig,
		Parameters:  nil,
	}
	return context.WithValue(context.Background(), "stageContext", stageContext)
}

func TestConfirmableMessage(t *testing.T) {
	pipelineContext := getContext("coap://localhost:56831/sdc", POST, CONFIRMABLE)
	stageInstance, err := stagelibrary.CreateStageInstance(LIBRARY, STAGE_NAME)
	if err != nil {
		t.Error(err)
	}
	stageInstance.Init(pipelineContext)
	records := make([]api.Record, 1)
	records[0] = common.CreateRecord("1", "TestData")
	batch := runner.NewBatchImpl("random", records, "randomOffset")
	err = stageInstance.(api.Destination).Write(batch)
	if err == nil {
		t.Error("Excepted error message for invalid CoAP URL with confirmable message")
	}
	stageInstance.Destroy()
}

func TestNonConfirmableMessage(t *testing.T) {
	pipelineContext := getContext("coap://localhost:45/sdc", POST, NONCONFIRMABLE)
	stageInstance, err := stagelibrary.CreateStageInstance(LIBRARY, STAGE_NAME)
	if err != nil {
		t.Error(err)
	}
	records := make([]api.Record, 1)
	records[0] = common.CreateRecord("1", "test data")
	batch := runner.NewBatchImpl("random", records, "randomOffset")

	stageInstance.Init(pipelineContext)
	err = stageInstance.(api.Destination).Write(batch)
	if err != nil {
		t.Error("Not excepted error message for invalid CoAP URL with confirmable message")
	}
	stageInstance.Destroy()
}
