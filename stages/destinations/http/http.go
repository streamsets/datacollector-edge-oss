package http

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/lib/datagenerator"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"io/ioutil"
	"log"
	"net/http"
)

const (
	LIBRARY    = "streamsets-datacollector-basic-lib"
	STAGE_NAME = "com_streamsets_pipeline_stage_destination_http_HttpClientDTarget"
)

type HttpClientDestination struct {
	*common.BaseStage
	Conf HttpClientTargetConfig `ConfigDefBean:"conf"`
}

type HttpClientTargetConfig struct {
	ResourceUrl               string                                  `ConfigDef:"type=STRING,required=true"`
	Headers                   map[string]string                       `ConfigDef:"type=MAP,required=true"`
	SingleRequestPerBatch     bool                                    `ConfigDef:"type=BOOLEAN,required=true"`
	Client                    ClientConfigBean                        `ConfigDefBean:"client"`
	DataFormat                string                                  `ConfigDef:"type=STRING,required=true"`
	DataGeneratorFormatConfig datagenerator.DataGeneratorFormatConfig `ConfigDefBean:"dataGeneratorFormatConfig"`
}

type ClientConfigBean struct {
	HttpCompression string        `ConfigDef:"type=STRING,required=true"`
	TlsConfig       TlsConfigBean `ConfigDefBean:"tlsConfig"`
}

type TlsConfigBean struct {
	TlsEnabled         bool   `ConfigDef:"type=BOOLEAN,required=true"`
	TrustStoreFilePath string `ConfigDef:"type=STRING,required=true"`
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &HttpClientDestination{BaseStage: &common.BaseStage{}}
	})
}

func (h *HttpClientDestination) Init(stageContext api.StageContext) error {
	var err error
	if err = h.BaseStage.Init(stageContext); err != nil {
		return err
	}
	log.Println("[DEBUG] HttpClientDestination Init method")
	return h.Conf.DataGeneratorFormatConfig.Init(h.Conf.DataFormat)
}

func (h *HttpClientDestination) Write(batch api.Batch) error {
	log.Println("[DEBUG] HttpClientDestination write method")
	if h.Conf.SingleRequestPerBatch && len(batch.GetRecords()) > 0 {
		return h.writeSingleRequestPerBatch(batch)
	} else {
		return h.writeSingleRequestPerRecord(batch)
	}
}

func (h *HttpClientDestination) writeSingleRequestPerBatch(batch api.Batch) error {
	var err error
	recordWriterFactory := h.Conf.DataGeneratorFormatConfig.RecordWriterFactory
	batchBuffer := bytes.NewBuffer([]byte{})
	recordWriter, err := recordWriterFactory.CreateWriter(h.GetStageContext(), batchBuffer)
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
	recordWriterFactory := h.Conf.DataGeneratorFormatConfig.RecordWriterFactory
	for _, record := range batch.GetRecords() {
		recordBuffer := bytes.NewBuffer([]byte{})
		recordWriter, err := recordWriterFactory.CreateWriter(h.GetStageContext(), recordBuffer)
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

	if h.Conf.Client.HttpCompression == "GZIP" {
		gz := gzip.NewWriter(&buf)
		if _, err := gz.Write(jsonValue); err != nil {
			return err
		}
		gz.Close()
	} else {
		buf = *bytes.NewBuffer(jsonValue)
	}

	req, err := http.NewRequest("POST", h.Conf.ResourceUrl, &buf)
	if h.Conf.Headers != nil {
		for key, value := range h.Conf.Headers {
			req.Header.Set(key, value)
		}
	}

	req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	if h.Conf.Client.HttpCompression == "GZIP" {
		req.Header.Set("Content-Encoding", "gzip")
	}

	var client *http.Client

	if h.Conf.Client.TlsConfig.TlsEnabled {
		caCert, err := ioutil.ReadFile(h.Conf.Client.TlsConfig.TrustStoreFilePath)
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
