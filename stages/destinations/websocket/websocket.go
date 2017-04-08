package websocket

import (
	"context"
	"encoding/json"
	"github.com/gorilla/websocket"
	"github.com/streamsets/dataextractor/api"
	"github.com/streamsets/dataextractor/container/common"
	"log"
	"net/http"
)

type WebSocketClientDestination struct {
	resourceUrl string
	headers     []interface{}
}

func (w *WebSocketClientDestination) Init(ctx context.Context) {
	stageContext := (ctx.Value("stageContext")).(common.StageContext)
	stageConfig := stageContext.StageConfig
	log.Println("WebSocketClientDestination Init method")
	for _, config := range stageConfig.Configuration {
		if config.Name == "conf.resourceUrl" {
			w.resourceUrl = config.Value.(string)
		}

		if config.Name == "conf.headers" {
			w.headers = config.Value.([]interface{})
		}
	}
}

func (w *WebSocketClientDestination) Write(batch api.Batch) error {
	log.Println("WebSocketClientDestination write method = " + w.resourceUrl)

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
		jsonValue, err := json.Marshal(record.Value)
		if err != nil {
			panic(err)
		}

		err = c.WriteMessage(websocket.TextMessage, jsonValue)
		if err != nil {
			log.Println("write:", err)
		}
	}

	defer c.Close()
	return nil
}

func (w *WebSocketClientDestination) Destroy() {

}
