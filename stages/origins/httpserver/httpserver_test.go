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
package httpserver

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/go-cleanhttp"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"github.com/streamsets/datacollector-edge/stages/lib"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

func getStageContext(
	configs []common.Config,
	parameters map[string]interface{},
) (*common.StageContextImpl, *common.ErrorSink) {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = Library
	stageConfig.StageName = StageName
	stageConfig.Configuration = configs
	errorSink := common.NewErrorSink()
	return &common.StageContextImpl{
		StageConfig: &stageConfig,
		Parameters:  parameters,
		ErrorSink:   errorSink,
	}, errorSink
}

func TestHttpServerOrigin_Init(t *testing.T) {
	portNumber := float64(500)
	appId := "edge"

	configs := []common.Config{
		{
			Name:  "httpConfigs.port",
			Value: portNumber,
		},
		{
			Name:  "httpConfigs.appId",
			Value: appId,
		},
		{
			Name:  "dataFormat",
			Value: "JSON",
		},
	}

	stageContext, _ := getStageContext(configs, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}

	stageInstance := stageBean.Stage
	if stageInstance == nil {
		t.Error("Failed to create stage instance")
	}

	if stageInstance.(*Origin).HttpConfigs.Port != portNumber {
		t.Error("Failed to inject config value for port number")
	}

	if stageInstance.(*Origin).HttpConfigs.AppId != appId {
		t.Error("Failed to inject config value for port number")
	}

	issues := stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Error(issues[0].Message)
	}

	err = stageInstance.Destroy()
	if err != nil {
		t.Fatal(err)
	}
}

func TestOrigin_Produce_JSON(t *testing.T) {
	freePort, _ := lib.GetFreePort()
	configs := []common.Config{
		{
			Name:  "httpConfigs.port",
			Value: float64(freePort),
		},
		{
			Name:  "httpConfigs.appId",
			Value: "edge",
		},
		{
			Name:  "dataFormat",
			Value: "JSON",
		},
	}

	stageContext, _ := getStageContext(configs, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}

	stageInstance := stageBean.Stage

	issues := stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Fatal(issues[0].Message)
	}

	go func() {
		httpServerUrl := fmt.Sprintf("http://localhost:%d", freePort)
		message := map[string]interface{}{
			"hello": "world",
			"life":  42,
			"embedded": map[string]string{
				"yes": "of course!",
			},
		}

		bytesRepresentation, err := json.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}

		httpClient := http.Client{}
		req, err := http.NewRequest("POST", httpServerUrl, bytes.NewBuffer(bytesRepresentation))
		req.Header.Set(X_SDC_APPLICATION_ID_HEADER, "edge")
		_, err = httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
	}()

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	_, err = stageInstance.(api.Origin).Produce(&stringOffset, 1, batchMaker)
	if err != nil {
		t.Error("Err :", err)
		return
	}

	records := batchMaker.GetStageOutput()
	if len(records) != 1 {
		t.Fatal("Expected 1 records but got - ", len(records))
	}

	helloField, _ := records[0].Get("/hello")
	if helloField.Value != "world" {
		t.Error("Expected 'world' but got - ", helloField.Value)
	}

	_ = stageInstance.Destroy()
}

func TestOrigin_Produce_Text_WithQueryAppId(t *testing.T) {
	freePort, _ := lib.GetFreePort()
	configs := []common.Config{
		{
			Name:  "httpConfigs.port",
			Value: float64(freePort),
		},
		{
			Name:  "httpConfigs.appId",
			Value: "edge",
		},
		{
			Name:  "httpConfigs.appIdViaQueryParamAllowed",
			Value: true,
		},
		{
			Name:  "dataFormat",
			Value: "TEXT",
		},
		{
			Name:  "dataFormatConfig.textMaxLineLen",
			Value: float64(1024),
		},
	}

	stageContext, _ := getStageContext(configs, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}

	stageInstance := stageBean.Stage

	issues := stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Fatal(issues[0].Message)
	}

	go func() {
		httpServerUrl := fmt.Sprintf("http://localhost:%d?sdcApplicationId=edge", freePort)
		message := "Hello World"
		httpClient := http.Client{}
		req, err := http.NewRequest("POST", httpServerUrl, bytes.NewBufferString(message))
		_, err = httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
	}()

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	_, err = stageInstance.(api.Origin).Produce(&stringOffset, 1, batchMaker)
	if err != nil {
		t.Error("Err :", err)
		return
	}

	records := batchMaker.GetStageOutput()
	if len(records) != 1 {
		t.Fatal("Expected 1 records but got - ", len(records))
	}

	textField, _ := records[0].Get("/text")
	if textField.Value != "Hello World" {
		t.Error("Expected 'Hello World' but got - ", textField.Value)
	}

	_ = stageInstance.Destroy()
}

func TestOrigin_Produce_InvalidAppId(t *testing.T) {
	freePort, _ := lib.GetFreePort()
	configs := []common.Config{
		{
			Name:  "httpConfigs.port",
			Value: float64(freePort),
		},
		{
			Name:  "httpConfigs.appId",
			Value: "edge",
		},
		{
			Name:  "dataFormat",
			Value: "TEXT",
		},
	}

	stageContext, _ := getStageContext(configs, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}

	stageInstance := stageBean.Stage

	issues := stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Fatal(issues[0].Message)
	}

	httpServerUrl := fmt.Sprintf("http://localhost:%d", freePort)
	message := "Hello World"
	httpClient := http.Client{}
	req, err := http.NewRequest("POST", httpServerUrl, bytes.NewBufferString(message))
	req.Header.Set(X_SDC_APPLICATION_ID_HEADER, "invalidAppID")
	resp, err := httpClient.Do(req)
	if resp != nil && resp.StatusCode != http.StatusForbidden {
		t.Fatal("Excepted 403 status code for invalid app Id")
	}

	_ = stageInstance.Destroy()
}

func TestOrigin_Produce_HTTPS(t *testing.T) {
	keyStoreFilePath, _ := filepath.Abs("test/myp12.p12")
	freePort, _ := lib.GetFreePort()
	configs := []common.Config{
		{
			Name:  "httpConfigs.port",
			Value: float64(freePort),
		},
		{
			Name:  "httpConfigs.appId",
			Value: "edge",
		},
		{
			Name:  "dataFormat",
			Value: "JSON",
		},
		{
			Name:  "httpConfigs.tlsConfigBean.tlsEnabled",
			Value: true,
		},
		{
			Name:  "httpConfigs.tlsConfigBean.keyStoreFilePath",
			Value: keyStoreFilePath,
		},
		{
			Name:  "httpConfigs.tlsConfigBean.keyStoreType",
			Value: "PKCS12",
		},
		{
			Name:  "httpConfigs.tlsConfigBean.keyStorePassword",
			Value: "password",
		},
	}

	stageContext, _ := getStageContext(configs, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters, nil)
	if err != nil {
		t.Error(err)
	}

	stageInstance := stageBean.Stage

	issues := stageInstance.Init(stageContext)
	if len(issues) > 0 {
		t.Fatal(issues[0].Message)
	}

	go func() {
		time.Sleep(100 * time.Millisecond)
		httpServerUrl := fmt.Sprintf("https://localhost:%d", freePort)
		message := map[string]interface{}{
			"hello": "world",
			"life":  42,
			"embedded": map[string]string{
				"yes": "of course!",
			},
		}

		bytesRepresentation, err := json.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}

		var caCertPool *x509.CertPool
		certPemFilePath, _ := filepath.Abs("test/mypem.pem")

		caCert, err := ioutil.ReadFile(certPemFilePath)
		if err != nil {
			t.Fatal(err)
		}

		// appending to the system cert pool rather than replacing it
		caCertPool, err = x509.SystemCertPool()
		if err != nil {
			t.Fatal(err)
		}

		if !caCertPool.AppendCertsFromPEM(caCert) {
			t.Fatal("Error adding ca certificate")
		}

		httpTransport := cleanhttp.DefaultTransport()
		httpTransport.TLSClientConfig = &tls.Config{
			RootCAs: caCertPool,
		}
		httpClient := &http.Client{
			Transport: httpTransport,
		}

		req, err := http.NewRequest("POST", httpServerUrl, bytes.NewBuffer(bytesRepresentation))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set(X_SDC_APPLICATION_ID_HEADER, "edge")
		_, err = httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
	}()

	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{}, false)
	_, err = stageInstance.(api.Origin).Produce(&stringOffset, 1, batchMaker)
	if err != nil {
		t.Error("Err :", err)
		return
	}

	records := batchMaker.GetStageOutput()
	if len(records) != 1 {
		t.Fatal("Expected 1 records but got - ", len(records))
	}

	helloField, _ := records[0].Get("/hello")
	if helloField.Value != "world" {
		t.Error("Expected 'world' but got - ", helloField.Value)
	}

	_ = stageInstance.Destroy()
}
