package http

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/recordio"
	"github.com/streamsets/datacollector-edge/container/recordio/jsonrecord"
	"github.com/streamsets/datacollector-edge/container/recordio/textrecord"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
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
	*common.BaseStage
	resourceUrl           string
	headers               []interface{}
	singleRequestPerBatch bool
	httpCompression       string
	tlsEnabled            bool
	trustStoreFilePath    string
	dataFormat            string
	recordWriterFactory   recordio.RecordWriterFactory
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &HttpClientDestination{BaseStage: &common.BaseStage{}}
	})
}

func (h *HttpClientDestination) Init(stageContext api.StageContext) error {
	if err := h.BaseStage.Init(stageContext); err != nil {
		return err
	}
	stageConfig := h.GetStageConfig()
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

		if config.Name == "conf.client.tlsConfig.tlsEnabled" {
			h.tlsEnabled = stageContext.GetResolvedValue(config.Value).(bool)
		}

		if config.Name == "conf.client.tlsConfig.trustStoreFilePath" && config.Value != nil {
			h.trustStoreFilePath = stageContext.GetResolvedValue(config.Value).(string)
		}

		if config.Name == "conf.dataFormat" && config.Value != nil {
			h.dataFormat = stageContext.GetResolvedValue(config.Value).(string)
		}

	}

	switch h.dataFormat {
	case "TEXT":
		h.recordWriterFactory = &textrecord.TextWriterFactoryImpl{}
	case "JSON":
		h.recordWriterFactory = &jsonrecord.JsonWriterFactoryImpl{}
	default:
		return errors.New("Unsupported Data Format - " + h.dataFormat)
	}

	return nil
}

func (h *HttpClientDestination) Write(batch api.Batch) error {
	log.Println("[DEBUG] HttpClientDestination write method")
	if h.singleRequestPerBatch && len(batch.GetRecords()) > 0 {
		return h.writeSingleRequestPerBatch(batch)
	} else {
		return h.writeSingleRequestPerRecord(batch)
	}
}

func (h *HttpClientDestination) writeSingleRequestPerBatch(batch api.Batch) error {
	var err error
	batchBuffer := bytes.NewBuffer([]byte{})
	recordWriter, err := h.recordWriterFactory.CreateWriter(h.GetStageContext(), batchBuffer)
	if err != nil {
		return err
	}
	for _, record := range batch.GetRecords() {
		err = recordWriter.WriteRecord(record)
		if err != nil {
			return err
		}
	}
	recordWriter.Flush()
	recordWriter.Close()
	return h.sendToSDC(batchBuffer.Bytes())
}

func (h *HttpClientDestination) writeSingleRequestPerRecord(batch api.Batch) error {
	var err error
	for _, record := range batch.GetRecords() {
		recordBuffer := bytes.NewBuffer([]byte{})
		recordWriter, err := h.recordWriterFactory.CreateWriter(h.GetStageContext(), recordBuffer)
		if err != nil {
			return err
		}
		err = recordWriter.WriteRecord(record)
		if err != nil {
			return err
		}
		recordWriter.Flush()
		recordWriter.Close()
		err = h.sendToSDC(recordBuffer.Bytes())
		if err != nil {
			return err
		}
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

	var client *http.Client

	if h.tlsEnabled {
		caCert, err := ioutil.ReadFile(h.trustStoreFilePath)
		if err != nil {
			return err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		client = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					RootCAs:            caCertPool,
					InsecureSkipVerify: true,
				},
			},
		}
	} else {
		client = &http.Client{}
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	log.Println("[DEBUG] response Status:", resp.Status)
	if resp.StatusCode != 200 {
		return errors.New(resp.Status)
	}

	return nil
}
