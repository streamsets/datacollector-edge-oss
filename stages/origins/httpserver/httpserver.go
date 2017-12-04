/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package httpserver

import (
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

const (
	LIBRARY    = "streamsets-datacollector-basic-lib"
	STAGE_NAME = "com_streamsets_pipeline_stage_origin_httpserver_HttpServerDPushSource"
)

type HttpServerOrigin struct {
	*common.BaseStage
	HttpConfigs  RawHttpConfigs `ConfigDefBean:"name=httpConfigs"`
	httpServer   *http.Server
	incomingData chan interface{}
}

type RawHttpConfigs struct {
	Port  float64 `ConfigDef:"type=NUMBER,required=true"`
	AppId string  `ConfigDef:"type=STRING,required=true"`
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &HttpServerOrigin{BaseStage: &common.BaseStage{}}
	})
}

func (h *HttpServerOrigin) Init(stageContext api.StageContext) error {
	if err := h.BaseStage.Init(stageContext); err != nil {
		return err
	}
	h.httpServer = h.startHttpServer()
	h.incomingData = make(chan interface{})
	return nil
}

func (h *HttpServerOrigin) Destroy() error {
	if err := h.httpServer.Shutdown(nil); err != nil {
		return err
	}
	log.Debug("HTTP Server - server shutdown successfully")
	return nil
}

func (h *HttpServerOrigin) Produce(
	lastSourceOffset string,
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (string, error) {
	log.Debug("HTTP Server - Produce method")
	value := <-h.incomingData
	log.WithField("value", value).Debug("Incoming Data")
	record, _ := h.GetStageContext().CreateRecord(time.Now().String(), value)
	batchMaker.AddRecord(record)
	return "", nil
}

func (h *HttpServerOrigin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.WithError(err).Error("HTTP Server error reading request body")
		h.GetStageContext().ReportError(err)
	} else {
		h.incomingData <- string(body)
	}
}

func (h *HttpServerOrigin) startHttpServer() *http.Server {
	srv := &http.Server{
		Addr:    ":" + strconv.FormatFloat(h.HttpConfigs.Port, 'E', -1, 64),
		Handler: h,
	}

	go func() {
		log.Debug("HTTP Server - Running on URI : http://localhost:", h.HttpConfigs.Port)
		if err := srv.ListenAndServe(); err != nil {
			log.WithError(err).Error("HttpServer: ListenAndServe() error")
			h.GetStageContext().ReportError(err)
		}
	}()

	return srv
}
