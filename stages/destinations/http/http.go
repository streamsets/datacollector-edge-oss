package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/streamsets/dataextractor/api"
	"github.com/streamsets/dataextractor/container/common"
	"io/ioutil"
	"net/http"
)

const DEBUG = false

type HttpClientDestination struct {
	resourceUrl string
	headers     []interface{}
}

func (h *HttpClientDestination) Init(stageConfig common.StageConfiguration) {
	fmt.Println("HttpClientDestination Init method")
	for _, config := range stageConfig.Configuration {
		if config.Name == "conf.resourceUrl" {
			h.resourceUrl = config.Value.(string)
		}

		if config.Name == "conf.headers" {
			h.headers = config.Value.([]interface{})
		}
	}
}

func (h *HttpClientDestination) Write(batch api.Batch) error {
	fmt.Println("HttpClientDestination write method")
	for _, record := range batch.GetRecords() {
		h.sendRecordToSDC(record.Value)
	}
	return nil
}

func (h *HttpClientDestination) sendRecordToSDC(recordValue interface{}) {
	if DEBUG {
		fmt.Println("Start sending record")
		fmt.Println(recordValue)
		fmt.Println("URL:>", h.resourceUrl)
	}

	jsonValue, err := json.Marshal(recordValue)
	if err != nil {
		panic(err)
	}

	req, err := http.NewRequest("POST", h.resourceUrl, bytes.NewBuffer(jsonValue))
	if h.headers != nil {
		for _, header := range h.headers {
			req.Header.Set(header.(map[string]interface{})["key"].(string),
				header.(map[string]interface{})["value"].(string))
		}
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	if DEBUG {
		fmt.Println("response Status:", resp.Status)
		fmt.Println("response Headers:", resp.Header)
		body, _ := ioutil.ReadAll(resp.Body)
		fmt.Println("response Body:", string(body))
	}
}

func (h *HttpClientDestination) Destroy() {

}
