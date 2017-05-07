package http

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"github.com/streamsets/dataextractor/api"
	"github.com/streamsets/dataextractor/container/common"
	"github.com/streamsets/dataextractor/stages/stagelibrary"
	"io/ioutil"
	"log"
	"net/http"
)

const (
	DEBUG      = false
	LIBRARY    = "streamsets-datacollector-basic-lib"
	STAGE_NAME = "com_streamsets_pipeline_stage_destination_http_HttpClientDTarget"
)

type HttpClientDestination struct {
	ctx                   context.Context
	resourceUrl           string
	headers               []interface{}
	singleRequestPerBatch bool
	httpCompression       string
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &HttpClientDestination{}
	})
}

func (h *HttpClientDestination) Init(ctx context.Context) {
	stageContext := (ctx.Value("stageContext")).(common.StageContext)
	stageConfig := stageContext.StageConfig
	log.Println("[DEBUG] HttpClientDestination Init method")
	for _, config := range stageConfig.Configuration {
		if config.Name == "conf.resourceUrl" {
			h.resourceUrl = stageContext.GetResolvedValue(config.Value).(string)
		}

		if config.Name == "conf.headers" {
			h.headers = stageContext.GetResolvedValue(config.Value).([]interface{})
		}

		if config.Name == "conf.singleRequestPerBatch" {
			h.singleRequestPerBatch = stageContext.GetResolvedValue(config.Value).(bool)
		}

		if config.Name == "conf.client.httpCompression" {
			h.httpCompression = stageContext.GetResolvedValue(config.Value).(string)
		}
	}
}

func (h *HttpClientDestination) Write(batch api.Batch) error {
	log.Println("[DEBUG] HttpClientDestination write method")
	var err error
	var batchByteArray []byte
	for _, record := range batch.GetRecords() {

		var recordByteArray []byte
		switch record.Value.(type) {
		case string:
			recordByteArray = []byte(record.Value.(string))
		default:
			recordByteArray, err = json.Marshal(record.Value)
			if err != nil {
				return err
			}
		}

		if h.singleRequestPerBatch {
			batchByteArray = append(batchByteArray, recordByteArray...)
			batchByteArray = append(batchByteArray, "\n"...)
		} else {
			err = h.sendToSDC(recordByteArray)
			if err != nil {
				return err
			}
		}
	}
	if h.singleRequestPerBatch && len(batch.GetRecords()) > 0 {
		err = h.sendToSDC(batchByteArray)
	}
	return err
}

func (h *HttpClientDestination) sendToSDC(jsonValue []byte) error {
	var buf bytes.Buffer

	if h.httpCompression == "GZIP" {
		gz := gzip.NewWriter(&buf)
		if _, err := gz.Write(jsonValue); err != nil {
			return err
		}
		gz.Close()
	} else {
		buf = *bytes.NewBuffer(jsonValue)
	}

	req, err := http.NewRequest("POST", h.resourceUrl, &buf)
	if h.headers != nil {
		for _, header := range h.headers {
			req.Header.Set(header.(map[string]interface{})["key"].(string),
				header.(map[string]interface{})["value"].(string))
		}
	}

	req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	if h.httpCompression == "GZIP" {
		req.Header.Set("Content-Encoding", "gzip")
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	log.Println("[DEBUG] response Status:", resp.Status)
	if resp.StatusCode != 200 {
		return errors.New(resp.Status)
	}

	if DEBUG {
		log.Println("[DEBUG] response Status:", resp.Status)
		log.Println("[DEBUG] response Headers:", resp.Header)
		body, _ := ioutil.ReadAll(resp.Body)
		log.Println("[DEBUG] response Body:", string(body))
	}

	return nil
}

func (h *HttpClientDestination) Destroy() {

}
