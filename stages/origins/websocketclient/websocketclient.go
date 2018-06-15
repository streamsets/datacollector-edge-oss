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
package websocketclient

import (
	"bytes"
	"fmt"
	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/lib/dataparser"
	"github.com/streamsets/datacollector-edge/stages/lib/httpcommon"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"net/http"
)

const (
	Library               = "streamsets-datacollector-basic-lib"
	StageName             = "com_streamsets_pipeline_stage_origin_websocket_WebSocketClientDSource"
	ConnectionClosedError = "connection closed, code: %d, message: %s"
)

var defaultOffset = "webSocket"

type Origin struct {
	*common.BaseStage
	Conf            OriginClientConfig `ConfigDefBean:"conf"`
	incomingRecords chan api.Record
	webSocketConn   *websocket.Conn
	destroyed       chan bool
}

type OriginClientConfig struct {
	ResourceUrl      string                            `ConfigDef:"type=STRING,required=true"`
	Headers          map[string]string                 `ConfigDef:"type=MAP,required=true"`
	RequestBody      string                            `ConfigDef:"type=STRING,required=true"`
	BasicAuth        httpcommon.PasswordAuthConfigBean `ConfigDefBean:"basicAuth"`
	DataFormat       string                            `ConfigDef:"type=STRING,required=true"`
	DataFormatConfig dataparser.DataParserFormatConfig `ConfigDefBean:"dataFormatConfig"`
}

func init() {
	stagelibrary.SetCreator(Library, StageName, func() api.Stage {
		return &Origin{BaseStage: &common.BaseStage{}}
	})
}

func (o *Origin) Init(stageContext api.StageContext) []validation.Issue {
	issues := o.BaseStage.Init(stageContext)
	o.incomingRecords = make(chan api.Record)
	issues = o.Conf.DataFormatConfig.Init(o.Conf.DataFormat, stageContext, issues)
	o.destroyed = make(chan bool)
	if len(issues) == 0 {
		// Initialize WebSocket connection
		requestHeader := http.Header{}
		for key, value := range o.Conf.Headers {
			requestHeader.Set(key, value)
		}

		var err error
		o.webSocketConn, _, err = websocket.DefaultDialer.Dial(o.Conf.ResourceUrl, requestHeader)
		if err != nil {
			issues = append(issues, stageContext.CreateConfigIssue(err.Error()))
			return issues
		}

		o.webSocketConn.SetCloseHandler(o.closeHandler)

		if len(o.Conf.RequestBody) > 0 {
			o.webSocketConn.WriteMessage(websocket.TextMessage, []byte(o.Conf.RequestBody))
		}
		go o.messageHandler()

	}
	return issues
}

func (o *Origin) Produce(lastSourceOffset *string, maxBatchSize int, batchMaker api.BatchMaker) (*string, error) {
	record := <-o.incomingRecords
	if record != nil {
		batchMaker.AddRecord(record)
	}
	return &defaultOffset, nil
}

func (o *Origin) Destroy() error {
	log.Debug("WebSocket Client Origin Destroy method")
	if o.webSocketConn != nil {
		o.webSocketConn.Close()
	}
	o.destroyed <- true
	return nil
}

func (o *Origin) messageHandler() {
	for {
		select {
		case <-o.destroyed:
			log.Debug("WebSocket Client Origin destroyed channel called")
			return
		default:
			_, message, err := o.webSocketConn.ReadMessage()
			if err != nil {
				o.GetStageContext().ReportError(err)
			}
			o.processMessage(message)
		}
	}
}

func (o *Origin) processMessage(message []byte) {
	recordReaderFactory := o.Conf.DataFormatConfig.RecordReaderFactory
	recordBuffer := bytes.NewBuffer(message)
	recordReader, err := recordReaderFactory.CreateReader(o.GetStageContext(), recordBuffer, "webSocket")
	if err != nil {
		o.GetStageContext().ReportError(err)
		log.WithError(err).Error("Failed to create record reader")
	}
	defer recordReader.Close()

	for {
		record, err := recordReader.ReadRecord()
		if err != nil {
			o.GetStageContext().ReportError(err)
			log.WithError(err).Error("Failed to parse raw data")
		}
		if record == nil {
			break
		}
		o.incomingRecords <- record
	}
}

func (o *Origin) closeHandler(code int, message string) error {
	log.WithField("code", code).WithField("message", message).Error("Connection Closed")
	o.GetStageContext().ReportError(fmt.Errorf(ConnectionClosedError, code, message))
	return nil
}
