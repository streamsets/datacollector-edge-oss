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
	"encoding/json"
	"fmt"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"net/http"
	"net/http/httptest"
	"testing"
)

const (
	GET  = "GET"
	POST = "POST"
)

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
		StageConfig:       &stageConfig,
		Parameters:        parameters,
		ErrorSink:         errorSink,
		ErrorRecordPolicy: common.ErrorRecordPolicyStage,
	}
}

func TestProcessor_Init(t *testing.T) {
	outputField := "/responseField"
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
			Name:  "conf.outputField",
			Value: outputField,
		},
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

	if stageInstance.(*Processor).Conf.OutputField != outputField {
		t.Error("Failed to inject config value for outputField")
	}

	if stageInstance.(*Processor).Conf.ResourceUrl != resourceUrl {
		t.Error("Failed to inject config value for resourceUrl")
	}

	if stageInstance.(*Processor).Conf.Headers == nil {
		t.Error("Failed to inject config value for Headers")
		return
	}

	if stageInstance.(*Processor).Conf.Headers["X-SDC-APPLICATION-ID"] != "SDC Edge" {
		t.Error("Failed to inject config value for Headers")
	}

	if stageInstance.(*Processor).Conf.Headers["DUMMY-HEADER"] != "DUMMY" {
		t.Error("Failed to inject config value for Headers")
	}
}

func TestHttpClientProcessor_InvalidURL(t *testing.T) {
	outputField := "/responseField"
	configuration := []common.Config{
		{
			Name:  "conf.outputField",
			Value: outputField,
		},
		{
			Name:  "conf.resourceUrl",
			Value: "http://invalidURL",
		},
		{
			Name:  "conf.dataFormat",
			Value: "JSON",
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

	records := make([]api.Record, 1)
	records[0], _ = stageContext.CreateRecord(
		"abc", map[string]interface{}{"a": float64(2.55), "b": float64(3.55), "c": "random"},
	)
	batch := runner.NewBatchImpl("random", records, nil)
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	err = stageInstance.(api.Processor).Process(batch, batchMaker)
	if err != nil {
		t.Error("Err :", err)
		return
	}

	if stageContext.ErrorSink.GetTotalErrorRecords() != 1 {
		t.Error("Expected error records related to invalid URL")
	}
}

func TestProcessor_Process_GET(t *testing.T) {
	// create test server to return JSON data
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sampleJSON := `
			{
				"status": "success"
			}
		`
		_, _ = fmt.Fprint(w, sampleJSON)
	}))
	defer ts.Close()

	outputField := "/responseField"

	configuration := []common.Config{
		{
			Name:  "conf.outputField",
			Value: outputField,
		},
		{
			Name:  "conf.resourceUrl",
			Value: ts.URL,
		},
		{
			Name:  "conf.dataFormat",
			Value: "JSON",
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
		t.Fatal("Failed to create stage instance")
	}

	issues := stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Fatal(issues)
	}

	records := make([]api.Record, 2)
	records[0], _ = stageContext.CreateRecord(
		"abc", map[string]interface{}{"a": float64(2.55), "b": float64(3.55), "c": "random"},
	)
	records[1], _ = stageContext.CreateRecord(
		"abc", map[string]interface{}{"a": float64(2.55), "b": float64(3.55), "c": "random"},
	)
	batch := runner.NewBatchImpl("random", records, nil)
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	err = stageInstance.(api.Processor).Process(batch, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	if stageContext.ErrorSink.GetTotalErrorRecords() != 0 {
		t.Errorf("Expected 0 error but got - %d", stageContext.ErrorSink.GetTotalErrorRecords())
		t.Fatal(stageContext.ErrorSink.GetErrorRecords()[""][0].GetHeader().GetErrorMessage())
	}

	processedRecords := batchMaker.GetStageOutput()
	if len(records) != 2 {
		t.Fatal("Expected 2 records but got - ", len(records))
	}

	rootField, _ := processedRecords[0].Get(outputField)
	mapFieldValue := rootField.Value.(map[string]*api.Field)
	if mapFieldValue["status"].Value != "success" {
		t.Error("Expected 'success' but got - ", rootField.Value)
	}

	_ = stageInstance.Destroy()
}

func TestProcessor_Process_EL(t *testing.T) {
	// create test server to return JSON data
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		decoder := json.NewDecoder(r.Body)
		var bodyParam interface{}
		_ = decoder.Decode(&bodyParam)

		response := map[string]interface{}{
			"status":        "success",
			"requestMethod": r.Method,
			"bodyParam":     bodyParam,
		}

		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "\t")
		_ = encoder.Encode(response)
	}))
	defer ts.Close()

	outputField := "/responseField"

	configuration := []common.Config{
		{
			Name:  "conf.outputField",
			Value: outputField,
		},
		{
			Name:  "conf.resourceUrl",
			Value: `${"` + ts.URL + `?a="+record:value('/a')}`,
		},
		{
			Name:  "conf.requestBody",
			Value: `${record:value('/b')}`,
		},
		{
			Name:  "conf.dataFormat",
			Value: "JSON",
		},
		{
			Name:  "conf.httpMethod",
			Value: Expression,
		},
		{
			Name:  "conf.methodExpression",
			Value: "${record:value('/method')}",
		},
		{
			Name:  "conf.defaultRequestContentType",
			Value: "text/html; charset=utf-8",
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
		t.Fatal("Failed to create stage instance")
	}

	issues := stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Fatal(issues)
	}

	records := make([]api.Record, 2)
	records[0], _ = stageContext.CreateRecord(
		"abc", map[string]interface{}{"a": float64(2.55), "b": "Body Param", "method": "POST"},
	)
	records[1], _ = stageContext.CreateRecord(
		"abc", map[string]interface{}{"a": float64(2.55), "b": "Body Param", "method": "POST"},
	)
	batch := runner.NewBatchImpl("random", records, nil)
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	err = stageInstance.(api.Processor).Process(batch, batchMaker)
	if err != nil {
		t.Error("Err :", err)
	}

	if stageContext.ErrorSink.GetTotalErrorRecords() != 0 {
		t.Errorf("Expected 0 error but got - %d", stageContext.ErrorSink.GetTotalErrorRecords())
		t.Fatal(stageContext.ErrorSink.GetErrorRecords()[""][0].GetHeader().GetErrorMessage())
	}

	processedRecords := batchMaker.GetStageOutput()
	if len(records) != 2 {
		t.Fatal("Expected 2 records but got - ", len(records))
	}

	rootField, _ := processedRecords[0].Get(outputField)
	mapFieldValue := rootField.Value.(map[string]*api.Field)
	if mapFieldValue["status"].Value != "success" {
		t.Error("Expected 'success' but got - ", rootField.Value)
	}

	_ = stageInstance.Destroy()
}
