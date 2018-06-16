// Copyright 2018 StreamSets Inc.
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
package httpclient

import (
	"bytes"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/lib/dataparser"
	"github.com/streamsets/datacollector-edge/stages/lib/httpcommon"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	Library         = "streamsets-datacollector-basic-lib"
	StageName       = "com_streamsets_pipeline_stage_origin_http_HttpClientDSource"
	Polling         = "POLLING"
	Streaming       = "STREAMING"
	Batch           = "BATCH"
	HTTP03ErrorCode = "Error fetching resource. Status Code: %s, Reason: %s"
	HTTP32ErrorCode = "Error executing request: %s"
)

var httpOffset = "http"

type HttpClientOrigin struct {
	*common.BaseStage
	*httpcommon.HttpCommon
	Conf HttpClientConfigBean `ConfigDefBean:"name=conf"`
}

type HttpClientConfigBean struct {
	ResourceUrl                 string                               `ConfigDef:"type=STRING,required=true"`
	Headers                     map[string]string                    `ConfigDef:"type=MAP,required=true"`
	HttpMethod                  string                               `ConfigDef:"type=STRING,required=true"`
	TimeZoneID                  string                               `ConfigDef:"type=STRING,required=true"`
	RequestBody                 string                               `ConfigDef:"type=STRING,required=true"`
	DefaultRequestContentType   string                               `ConfigDef:"type=STRING,required=true"`
	HttpMode                    string                               `ConfigDef:"type=STRING,required=true"`
	PollingInterval             float64                              `ConfigDef:"type=NUMBER,required=true"`
	Basic                       BasicConfig                          `ConfigDefBean:"name=basic"`
	Client                      httpcommon.ClientConfigBean          `ConfigDefBean:"client"`
	DataFormat                  string                               `ConfigDef:"type=STRING,required=true"`
	DataFormatConfig            dataparser.DataParserFormatConfig    `ConfigDefBean:"dataFormatConfig"`
	ResponseStatusActionConfigs []HttpStatusResponseActionConfigBean `ConfigDef:"type=MODEL,evaluation=EXPLICIT" ListBeanModel:"name=responseStatusActionConfigs"`
	ResponseTimeoutActionConfig HttpTimeoutResponseActionConfigBean  `ConfigDefBean:"responseTimeoutActionConfig"`
	Pagination                  PaginationConfigBean                 `ConfigDefBean:"pagination"`
}

type BasicConfig struct {
	MaxBatchSize float64 `ConfigDef:"type=NUMBER,required=true"`
	MaxWaitTime  float64 `ConfigDef:"type=NUMBER,required=true"`
}

type HttpStatusResponseActionConfigBean struct {
	StatusCode      float64 `ConfigDef:"type=NUMBER,required=true"`
	Action          string  `ConfigDef:"type=STRING,required=true"`
	BackoffInterval float64 `ConfigDef:"type=NUMBER,required=true"`
	MaxNumRetries   float64 `ConfigDef:"type=NUMBER,required=true"`
}

type HttpTimeoutResponseActionConfigBean struct {
	Action          string  `ConfigDef:"type=STRING,required=true"`
	BackoffInterval float64 `ConfigDef:"type=NUMBER,required=true"`
	MaxNumRetries   float64 `ConfigDef:"type=NUMBER,required=true"`
}

type PaginationConfigBean struct {
	Mode              string  `ConfigDef:"type=STRING,required=true"`
	NextPageFieldPath string  `ConfigDef:"type=STRING,required=true"`
	StopCondition     string  `ConfigDef:"type=STRING,required=true"`
	StartAt           float64 `ConfigDef:"type=NUMBER,required=true"`
	ResultFieldPath   string  `ConfigDef:"type=STRING,required=true"`
	KeepAllFields     bool    `ConfigDef:"type=BOOLEAN,required=true"`
	RateLimit         float64 `ConfigDef:"type=NUMBER,required=true"`
}

func init() {
	stagelibrary.SetCreator(Library, StageName, func() api.Stage {
		return &HttpClientOrigin{BaseStage: &common.BaseStage{}, HttpCommon: &httpcommon.HttpCommon{}}
	})
}

func (h *HttpClientOrigin) Init(stageContext api.StageContext) []validation.Issue {
	issues := h.BaseStage.Init(stageContext)
	if err := h.InitializeClient(h.Conf.Client); err != nil {
		issues = append(issues, stageContext.CreateConfigIssue(err.Error()))
		return issues
	}
	return h.Conf.DataFormatConfig.Init(h.Conf.DataFormat, h.GetStageContext(), issues)
}

func (h *HttpClientOrigin) Produce(
	lastSourceOffset *string,
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (*string, error) {
	log.Debug("HTTP Client - Produce method")
	switch h.Conf.HttpMode {
	case Polling:
		return h.pollModeProduce(lastSourceOffset, maxBatchSize, batchMaker)
	case Streaming:
		return h.streamingModeProduce(lastSourceOffset, maxBatchSize, batchMaker)
	default:
		return h.batchModeProduce(lastSourceOffset, maxBatchSize, batchMaker)
	}
}

func (h *HttpClientOrigin) pollModeProduce(
	lastSourceOffset *string,
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (*string, error) {
	time.Sleep(time.Duration(h.Conf.PollingInterval) * time.Millisecond)

	var buf bytes.Buffer

	req, err := http.NewRequest(h.Conf.HttpMethod, h.Conf.ResourceUrl, &buf)
	for key, value := range h.Conf.Headers {
		req.Header.Set(key, value)
	}

	resp, err := h.RoundTrip(req)
	if err != nil {
		h.GetStageContext().ReportError(fmt.Errorf(HTTP32ErrorCode, err.Error()))
		return &httpOffset, nil
	}
	defer resp.Body.Close()

	log.WithField("status", resp.Status).Debug("Response status")
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		bodyString := string(bodyBytes)
		h.GetStageContext().ReportError(fmt.Errorf(HTTP03ErrorCode, resp.Status, bodyString))
		return &httpOffset, nil
	}

	recordReaderFactory := h.Conf.DataFormatConfig.RecordReaderFactory

	recordReader, err := recordReaderFactory.CreateReader(h.GetStageContext(), resp.Body, "http")
	if err != nil {
		log.WithError(err).Error("Failed to create record reader")
		return &httpOffset, err
	}
	defer recordReader.Close()
	for {
		record, err := recordReader.ReadRecord()
		if err != nil {
			h.GetStageContext().ReportError(fmt.Errorf("Failed to parse raw data: %s", err.Error()))
			log.WithError(err).Error("Failed to parse raw data")
			return &httpOffset, nil
		}

		if record == nil {
			break
		}
		batchMaker.AddRecord(record)
	}

	return &httpOffset, nil
}

func (h *HttpClientOrigin) streamingModeProduce(
	lastSourceOffset *string,
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (*string, error) {
	var buf bytes.Buffer
	req, err := http.NewRequest(h.Conf.HttpMethod, h.Conf.ResourceUrl, &buf)
	for key, value := range h.Conf.Headers {
		req.Header.Set(key, value)
	}

	resp, err := h.RoundTrip(req)
	if err != nil {
		return &httpOffset, err
	}
	defer resp.Body.Close()

	log.WithField("status", resp.Status).Debug("Response status")

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		bodyString := string(bodyBytes)
		return &httpOffset, fmt.Errorf(HTTP03ErrorCode, resp.Status, bodyString)
	}

	recordReaderFactory := h.Conf.DataFormatConfig.RecordReaderFactory

	recordReader, err := recordReaderFactory.CreateReader(h.GetStageContext(), resp.Body, "http")
	if err != nil {
		log.WithError(err).Error("Failed to create record reader")
		return &httpOffset, err
	}
	defer recordReader.Close()

	recordCount := float64(0)
	for {
		record, err := recordReader.ReadRecord()
		if err != nil {
			log.WithError(err).Error("Failed to parse raw data")
			return &httpOffset, err
		}

		if record != nil {
			batchMaker.AddRecord(record)
			recordCount++
		}

		if record == nil || recordCount >= h.Conf.Basic.MaxBatchSize {
			break
		}
	}
	return &httpOffset, nil
}

func (h *HttpClientOrigin) batchModeProduce(
	lastSourceOffset *string,
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (*string, error) {
	// TODO: Implement Batch mode ...
	// batch mode simply stops the pipeline (returns null offset) after the request finishes.
	// it was mostly intended for use with pagination, but doesn't require it. Once you run out of records in a
	// response you can just return nil offset for batch mode.
	return nil, errors.New("Batch Mode is not supported")
}
