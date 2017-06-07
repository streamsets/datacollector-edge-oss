package websocket

import (
	"encoding/json"
	"github.com/gorilla/websocket"
	"github.com/streamsets/dataextractor/api"
	"github.com/streamsets/dataextractor/container/common"
	"github.com/streamsets/dataextractor/stages/stagelibrary"
	"log"
	"net/http"
)

const (
	LIBRARY    = "streamsets-datacollector-basic-lib"
	STAGE_NAME = "com_streamsets_pipeline_stage_destination_websocket_WebSocketDTarget"
)

type WebSocketClientDestination struct {
	*common.BaseStage
	resourceUrl string
	headers     []interface{}
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &WebSocketClientDestination{BaseStage: &common.BaseStage{}}
	})
}

func (w *WebSocketClientDestination) Init(stageContext api.StageContext) error {
	if err:= w.BaseStage.Init(stageContext); err != nil {
		return err
	}
	stageConfig := w.GetStageConfig()
	log.Println("[DEBUG] WebSocketClientDestination Init method")
	for _, config := range stageConfig.Configuration {
		if config.Name == "conf.resourceUrl" {
			w.resourceUrl = config.Value.(string)
		}

		if config.Name == "conf.headers" {
			w.headers = config.Value.([]interface{})
		}
	}
	return nil
}

func (w *WebSocketClientDestination) Write(batch api.Batch) error {
	log.Println("[DEBUG] WebSocketClientDestination write method = " + w.resourceUrl)

	var requestHeader = http.Header{}
	if w.headers != nil {
		for _, headerInterface := range w.headers {
			requestHeader.Set(headerInterface.(map[string]interface{})["key"].(string),
				headerInterface.(map[string]interface{})["value"].(string))
		}
	}

	c, _, err := websocket.DefaultDialer.Dial(w.resourceUrl, requestHeader)
	if err != nil {
		log.Fatal("dial:", err)
	}

	for _, record := range batch.GetRecords() {
		jsonValue, err := json.Marshal(record.GetValue())
		if err != nil {
			panic(err)
		}

		err = c.WriteMessage(websocket.TextMessage, jsonValue)
		if err != nil {
			log.Println("[ERROR] write:", err)
		}
	}

	defer c.Close()
	return nil
}
