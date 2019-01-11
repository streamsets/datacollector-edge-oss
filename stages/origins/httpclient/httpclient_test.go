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
	"fmt"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"net/http"
	"net/http/httptest"
	"testing"
)

const GET = "GET"

func getStageContext(
	configuration []common.Config,
	parameters map[string]interface{},
) *common.StageContextImpl {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = Library
	stageConfig.StageName = StageName
	stageConfig.Configuration = configuration
	errorSink := common.NewErrorSink()
	return &common.StageContextImpl{
		StageConfig: &stageConfig,
		Parameters:  parameters,
		ErrorSink:   errorSink,
	}
}

func TestHttpClientOrigin_Init(t *testing.T) {
	resourceUrl := "http://test:9000"
	headers := make([]interface{}, 2)
	headers[0] = map[string]interface{}{
		"key":   "X-SDC-APPLICATION-ID",
		"value": "SDC Edge",
	}
	headers[1] = map[string]interface{}{
		"key":   "DUMMY-HEADER",
		"value": "DUMMY",
	}

	configuration := []common.Config{
		{
			Name:  "conf.resourceUrl",
			Value: resourceUrl,
		},
		{
			Name:  "conf.headers",
			Value: headers,
		},
		{
			Name:  "conf.dataFormat",
			Value: "JSON",
		},
		{
			Name:  "conf.httpMode",
			Value: Polling,
		},
		{
			Name:  "conf.httpMethod",
			Value: GET,
		},
	}

	stageContext := getStageContext(configuration, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage
	if stageInstance == nil {
		t.Error("Failed to create stage instance")
	}

	if stageInstance.(*HttpClientOrigin).Conf.ResourceUrl != resourceUrl {
		t.Error("Failed to inject config value for resourceUrl")
	}

	if stageInstance.(*HttpClientOrigin).Conf.Headers == nil {
		t.Error("Failed to inject config value for Headers")
		return
	}

	if stageInstance.(*HttpClientOrigin).Conf.Headers["X-SDC-APPLICATION-ID"] != "SDC Edge" {
		t.Error("Failed to inject config value for Headers")
	}

	if stageInstance.(*HttpClientOrigin).Conf.Headers["DUMMY-HEADER"] != "DUMMY" {
		t.Error("Failed to inject config value for Headers")
	}
}

func TestHttpClientOrigin_InvalidURL(t *testing.T) {
	configuration := []common.Config{
		{
			Name:  "conf.resourceUrl",
			Value: "http://invalidURL",
		},
		{
			Name:  "conf.dataFormat",
			Value: "JSON",
		},
		{
			Name:  "conf.httpMode",
			Value: Polling,
		},
		{
			Name:  "conf.httpMethod",
			Value: GET,
		},
	}

	stageContext := getStageContext(configuration, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}
	stageInstance := stageBean.Stage
	issues := stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Error(issues)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	_, err = stageInstance.(api.Origin).Produce(&httpOffset, 1, batchMaker)
	if err != nil {
		t.Error("Err :", err)
		return
	}

	if stageContext.ErrorSink.GetTotalErrorMessages() != 1 {
		t.Error("Expected error message related to invalid URL")
	}
}

func TestHttpClientOrigin_Produce_Polling_JSON(t *testing.T) {
	// create test server to return JSON data
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sampleJSON := `
			{
				"status": "success"
			}
			{
				"status": "failure"
			}
		`
		fmt.Fprint(w, sampleJSON)
	}))
	defer ts.Close()

	configuration := []common.Config{
		{
			Name:  "conf.resourceUrl",
			Value: ts.URL,
		},
		{
			Name:  "conf.dataFormat",
			Value: "JSON",
		},
		{
			Name:  "conf.httpMode",
			Value: Polling,
		},
		{
			Name:  "conf.httpMethod",
			Value: GET,
		},
	}

	stageContext := getStageContext(configuration, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage
	if stageInstance == nil {
		t.Error("Failed to create stage instance")
	}

	issues := stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Error(issues)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	_, err = stageInstance.(api.Origin).Produce(&httpOffset, 1, batchMaker)
	if err != nil {
		t.Error("Err :", err)
		return
	}

	records := batchMaker.GetStageOutput()
	if len(records) != 2 {
		t.Error("Expected 2 records but got - ", len(records))
		return
	}

	rootField, _ := records[0].Get()
	mapFieldValue := rootField.Value.(map[string]*api.Field)
	if mapFieldValue["status"].Value != "success" {
		t.Error("Expected 'success' but got - ", rootField.Value)
	}

	stageInstance.Destroy()
}

func TestHttpClientOrigin_Produce_Polling_TEXT(t *testing.T) {
	// create test server to return TEXT data
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sampleJSON := "line1\n line2\n line3"
		fmt.Fprint(w, sampleJSON)
	}))
	defer ts.Close()

	configuration := []common.Config{
		{
			Name:  "conf.resourceUrl",
			Value: ts.URL,
		},
		{
			Name:  "conf.dataFormat",
			Value: "TEXT",
		},
		{
			Name:  "conf.httpMode",
			Value: Polling,
		},
		{
			Name:  "conf.httpMethod",
			Value: GET,
		},
		{
			Name:  "conf.dataFormatConfig.textMaxLineLen",
			Value: float64(1024),
		},
	}
	stageContext := getStageContext(configuration, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage
	if stageInstance == nil {
		t.Error("Failed to create stage instance")
	}

	issues := stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Error(issues)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	_, err = stageInstance.(api.Origin).Produce(&httpOffset, 1, batchMaker)
	if err != nil {
		t.Error("Err :", err)
		return
	}

	records := batchMaker.GetStageOutput()
	if len(records) != 3 {
		t.Error("Expected 3 records but got - ", len(records))
		return
	}

	rootField, _ := records[0].Get()
	mapFieldValue := rootField.Value.(map[string]*api.Field)
	if mapFieldValue["text"].Value != "line1" {
		t.Error("Expected 'line1' but got - ", mapFieldValue["text"].Value)
	}

	stageInstance.Destroy()
}

func TestHttpClientOrigin_Produce_STREAMING_JSON(t *testing.T) {
	// create test server to return JSON data
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sampleJSON := `
			{
				"status": "success"
			}
		`
		for i := 0; i < 30; i++ {
			fmt.Fprint(w, sampleJSON)
		}
	}))
	defer ts.Close()

	configuration := []common.Config{
		{
			Name:  "conf.resourceUrl",
			Value: ts.URL,
		},
		{
			Name:  "conf.dataFormat",
			Value: "JSON",
		},
		{
			Name:  "conf.httpMode",
			Value: Streaming,
		},
		{
			Name:  "conf.httpMethod",
			Value: GET,
		},
		{
			Name:  "conf.basic.maxBatchSize",
			Value: float64(5),
		},
	}

	stageContext := getStageContext(configuration, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage
	if stageInstance == nil {
		t.Error("Failed to create stage instance")
	}

	issues := stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Error(issues)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	_, err = stageInstance.(api.Origin).Produce(&httpOffset, 1, batchMaker)
	if err != nil {
		t.Error("Err :", err)
		return
	}

	records := batchMaker.GetStageOutput()
	if len(records) != 5 {
		t.Error("Expected 5 records but got - ", len(records))
		return
	}

	rootField, _ := records[0].Get()
	mapFieldValue := rootField.Value.(map[string]*api.Field)
	if mapFieldValue["status"].Value != "success" {
		t.Error("Expected 'success' but got - ", rootField.Value)
	}

	stageInstance.Destroy()
}

func TestHttpClientOrigin_Error_Response(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Sample Server Error", 500)
	}))
	defer ts.Close()

	configuration := []common.Config{
		{
			Name:  "conf.resourceUrl",
			Value: ts.URL,
		},
		{
			Name:  "conf.dataFormat",
			Value: "TEXT",
		},
		{
			Name:  "conf.httpMode",
			Value: Polling,
		},
		{
			Name:  "conf.httpMethod",
			Value: GET,
		},
	}
	stageContext := getStageContext(configuration, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage
	if stageInstance == nil {
		t.Error("Failed to create stage instance")
	}

	issues := stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Error(issues)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	_, err = stageInstance.(api.Origin).Produce(&httpOffset, 1, batchMaker)
	if err != nil {
		t.Error("Err :", err)
		return
	}

	if stageContext.ErrorSink.GetTotalErrorMessages() != 1 {
		t.Error("Expected error message related to 500 error")
	}

	errorMessages := stageContext.ErrorSink.GetStageErrorMessages("")
	if errorMessages[0].LocalizableMessage != "Error fetching resource. Status Code: 500 Internal Server Error, Reason: Sample Server Error\n" {
		t.Errorf("Expected: 'Error fetching resource. Status Code: 500 Internal Server Error, Reason: Sample Server Error', but got: %s", errorMessages[0].LocalizableMessage)
	}

	records := batchMaker.GetStageOutput()
	if len(records) != 0 {
		t.Error("Expected 0 records but got - ", len(records))
		return
	}

	stageInstance.Destroy()
}

func TestHttpClientOrigin_Invalid_Data(t *testing.T) {
	// create test server to return JSON data
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sampleJSON := `
			{
				"status": "success"
		`
		fmt.Fprint(w, sampleJSON)
	}))
	defer ts.Close()

	configuration := []common.Config{
		{
			Name:  "conf.resourceUrl",
			Value: ts.URL,
		},
		{
			Name:  "conf.dataFormat",
			Value: "JSON",
		},
		{
			Name:  "conf.httpMode",
			Value: Polling,
		},
		{
			Name:  "conf.httpMethod",
			Value: GET,
		},
	}

	stageContext := getStageContext(configuration, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
		return
	}

	stageInstance := stageBean.Stage
	if stageInstance == nil {
		t.Error("Failed to create stage instance")
	}

	issues := stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Error(issues)
	}

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	_, err = stageInstance.(api.Origin).Produce(&httpOffset, 1, batchMaker)
	if err != nil {
		t.Error("Err :", err)
		return
	}

	if stageContext.ErrorSink.GetTotalErrorMessages() != 1 {
		t.Error("Expected error message related to data parser error")
	}

	errorMessages := stageContext.ErrorSink.GetStageErrorMessages("")
	if errorMessages[0].LocalizableMessage != "Failed to parse raw data: unexpected EOF" {
		t.Errorf("Expected: 'Failed to parse raw data: unexpected EOF', but got: %s", errorMessages[0].LocalizableMessage)
	}

	records := batchMaker.GetStageOutput()
	if len(records) != 0 {
		t.Error("Expected 0 records but got - ", len(records))
		return
	}

	stageInstance.Destroy()
}
