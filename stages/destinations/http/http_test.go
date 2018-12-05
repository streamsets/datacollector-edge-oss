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
package http

import (
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
)

func getStageContext(
	stageConfigurationList []common.Config,
	parameters map[string]interface{},
) *common.StageContextImpl {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = Library
	stageConfig.StageName = StageName
	stageConfig.Configuration = stageConfigurationList
	errorSink := common.NewErrorSink()
	return &common.StageContextImpl{
		StageConfig: &stageConfig,
		Parameters:  parameters,
		ErrorSink:   errorSink,
	}
}

func TestHttpClientDestination_Init(t *testing.T) {
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

	if stageInstance.(*HttpClientDestination).Conf.ResourceUrl != resourceUrl {
		t.Error("Failed to inject config value for resourceUrl")
	}

	if stageInstance.(*HttpClientDestination).Conf.Headers == nil {
		t.Error("Failed to inject config value for Headers")
		return
	}

	if stageInstance.(*HttpClientDestination).Conf.Headers["X-SDC-APPLICATION-ID"] != "SDC Edge" {
		t.Error("Failed to inject config value for Headers")
	}

	if stageInstance.(*HttpClientDestination).Conf.Headers["DUMMY-HEADER"] != "DUMMY" {
		t.Error("Failed to inject config value for Headers")
	}
}

func TestHttpClientDestination_Write(t *testing.T) {
	var requestData []byte
	// create test server to return JSON data
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Validate data
		requestData, _ = ioutil.ReadAll(r.Body)
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
		return
	}

	testData := make(map[string]interface{})
	testData["text"] = "Text Data"
	records := make([]api.Record, 1)
	records[0], _ = stageContext.CreateRecord("1", testData)
	batch := runner.NewBatchImpl("random", records, nil)
	err = stageInstance.(api.Destination).Write(batch)

	if stageContext.ErrorSink.GetTotalErrorMessages() != 0 {
		t.Errorf(
			"Expected no stage errors, but got erro: %s",
			stageContext.ErrorSink.GetStageErrorMessages("")[0].LocalizableMessage,
		)
		return
	}

	if string(requestData) != "Text Data\n" {
		t.Errorf("Failed to write, expected 'Text Data', but got: %s", string(requestData))
	}

	stageInstance.Destroy()
}
