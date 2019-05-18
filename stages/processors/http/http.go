// Copyright 2019 StreamSets Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package http

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/el"
	"github.com/streamsets/datacollector-edge/stages/lib/dataparser"
	"github.com/streamsets/datacollector-edge/stages/lib/httpcommon"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"io/ioutil"
	"net/http"
)

const (
	Library     = "streamsets-datacollector-basic-lib"
	StageName   = "com_streamsets_pipeline_stage_processor_http_HttpDProcessor"
	Version     = 1
	Expression  = "EXPRESSION"
	Header      = "HEADER"
	Field       = "FIELD"
	None        = "NONE"
	ErrorCode03 = "error fetching resource. Status Code: %s, Reason: %s"
	ErrorCode32 = "error executing request: %s"
)

type Processor struct {
	*common.BaseStage
	*httpcommon.HttpCommon
	Conf ProcessorConfig `ConfigDefBean:"conf"`
}

type ProcessorConfig struct {
	OutputField               string                            `ConfigDef:"type=STRING,required=true"`
	HeaderOutputLocation      string                            `ConfigDef:"type=STRING,required=true"`
	HeaderAttributePrefix     string                            `ConfigDef:"type=STRING,required=true"`
	HeaderOutputField         string                            `ConfigDef:"type=STRING,required=true,evaluation=EXPLICIT"`
	ResourceUrl               string                            `ConfigDef:"type=STRING,required=true,evaluation=EXPLICIT"`
	Headers                   map[string]string                 `ConfigDef:"type=MAP,required=true"`
	HttpMethod                string                            `ConfigDef:"type=STRING,required=true"`
	MethodExpression          string                            `ConfigDef:"type=STRING,required=true,evaluation=EXPLICIT"`
	RequestBody               string                            `ConfigDef:"type=STRING,required=true,evaluation=EXPLICIT"`
	DefaultRequestContentType string                            `ConfigDef:"type=STRING,required=true,evaluation=EXPLICIT"`
	RateLimit                 float64                           `ConfigDef:"type=NUMBER,required=true"`
	Client                    httpcommon.ClientConfigBean       `ConfigDefBean:"client"`
	DataFormat                string                            `ConfigDef:"type=STRING,required=true"`
	DataFormatConfig          dataparser.DataParserFormatConfig `ConfigDefBean:"dataFormatConfig"`
}

func init() {
	stagelibrary.SetCreator(Library, StageName, func() api.Stage {
		return &Processor{BaseStage: &common.BaseStage{}, HttpCommon: &httpcommon.HttpCommon{}}
	})
}

func (h *Processor) Init(stageContext api.StageContext) []validation.Issue {
	issues := h.BaseStage.Init(stageContext)
	if err := h.InitializeClient(h.Conf.Client); err != nil {
		issues = append(issues, stageContext.CreateConfigIssue(err.Error()))
		return issues
	}
	return h.Conf.DataFormatConfig.Init(h.Conf.DataFormat, h.GetStageContext(), issues)
}

func (h *Processor) Process(batch api.Batch, batchMaker api.BatchMaker) error {
	for _, record := range batch.GetRecords() {
		err := h.processRecord(record)
		if err != nil {
			h.GetStageContext().ToError(err, record)
		} else {
			batchMaker.AddRecord(record)
		}
	}
	return nil
}

func (h *Processor) processRecord(record api.Record) (err error) {
	recordContext := context.WithValue(context.Background(), el.RecordContextVar, record)

	resolvedHttpMethod, err := h.resolveHttpMethod(recordContext)
	if err != nil {
		return err
	}

	resolvedResourceUrl, err := h.resolveResourceUrl(recordContext)
	if err != nil {
		return err
	}

	resolveRequestedBody, err := h.resolveRequestedBody(recordContext)
	if err != nil {
		return err
	}

	resolvedContentType, err := h.resolveContentType(recordContext)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(resolvedHttpMethod, resolvedResourceUrl, resolveRequestedBody)
	if err != nil {
		return err
	}
	for key, value := range h.Conf.Headers {
		req.Header.Set(key, value)
	}
	req.Header.Set("Content-Type", resolvedContentType)

	resp, err := h.RoundTrip(req)
	if err != nil {
		return fmt.Errorf(ErrorCode32, err.Error())
	}
	defer resp.Body.Close()

	log.WithField("status", resp.Status).Debug("Response status")
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		bodyString := string(bodyBytes)
		return fmt.Errorf(ErrorCode03, resp.Status, bodyString)
	}

	recordReaderFactory := h.Conf.DataFormatConfig.RecordReaderFactory

	recordReader, err := recordReaderFactory.CreateReader(h.GetStageContext(), resp.Body, "http")
	if err != nil {
		log.WithError(err).Error("Failed to create record reader")
		return err
	}

	defer recordReader.Close()

	responseRecord, err := recordReader.ReadRecord()
	if err != nil {
		log.WithError(err).Error("Failed to parse raw data")
		return err
	}

	if outputField, err := responseRecord.Get(); err == nil {
		_, err = record.SetField(h.Conf.OutputField, outputField)
	}

	if h.Conf.HeaderOutputLocation == Header {
		for name, value := range resp.Header {
			if len(value) > 0 {
				record.GetHeader().SetAttribute(h.Conf.HeaderAttributePrefix+name, value[0])
			}
		}
	} else if h.Conf.HeaderOutputLocation == Field {
		headerMap := make(map[string]interface{})
		for name, value := range resp.Header {
			if len(value) > 0 {
				headerMap[name] = value[0]
			}
		}
		headerField, err := api.CreateMapField(headerMap)
		if err != nil {
			return err
		}
		_, err = record.SetField(h.Conf.HeaderOutputField, headerField)
	}

	return err
}

func (h *Processor) resolveHttpMethod(recordContext context.Context) (string, error) {
	if h.Conf.HttpMethod == Expression {
		result, err := h.GetStageContext().Evaluate(h.Conf.MethodExpression, "methodExpression", recordContext)
		if err != nil {
			return "", err
		}
		return cast.ToString(result), nil
	} else {
		return h.Conf.HttpMethod, nil
	}
}

func (h *Processor) resolveResourceUrl(recordContext context.Context) (string, error) {
	result, err := h.GetStageContext().Evaluate(h.Conf.ResourceUrl, "resourceUrl", recordContext)
	if err != nil {
		return "", err
	}
	return cast.ToString(result), nil
}

func (h *Processor) resolveRequestedBody(recordContext context.Context) (*bytes.Buffer, error) {
	var buf bytes.Buffer
	if len(h.Conf.RequestBody) > 0 {
		result, err := h.GetStageContext().Evaluate(h.Conf.RequestBody, "requestBody", recordContext)
		if err != nil {
			return nil, err
		}
		resolvedRequestedBody, err := json.Marshal(result)
		if err != nil {
			return nil, err
		}
		if h.Conf.Client.HttpCompression == "GZIP" {
			gz := gzip.NewWriter(&buf)
			if _, err := gz.Write(resolvedRequestedBody); err != nil {
				return &buf, err
			}
			_ = gz.Close()
		} else {
			buf = *bytes.NewBuffer(resolvedRequestedBody)
		}
	}
	return &buf, nil
}

func (h *Processor) resolveContentType(recordContext context.Context) (string, error) {
	result, err := h.GetStageContext().Evaluate(h.Conf.DefaultRequestContentType, "contentType", recordContext)
	if err != nil {
		return "", err
	}
	return cast.ToString(result), nil
}
