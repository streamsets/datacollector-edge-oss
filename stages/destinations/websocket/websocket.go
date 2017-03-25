package websocket

import (
	"fmt"
	"log"
	"github.com/streamsets/dataextractor/api"
	"github.com/streamsets/dataextractor/container/common"
	"github.com/gorilla/websocket"
	"encoding/json"
	"net/http"
)

type WebSocketClientDestination struct {
	resourceUrl string
	headers     []interface{}
}

func (w *WebSocketClientDestination) Init(stageConfig common.StageConfiguration) {
	fmt.Println("HttpClientDestination Init method")
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
	fmt.Println("WebSocketClientDestination write method = " + w.resourceUrl)


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
