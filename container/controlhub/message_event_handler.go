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
package controlhub

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/execution/manager"
	"github.com/streamsets/datacollector-edge/container/store"
	"io"
	"net/http"
	"runtime"
	"time"
)

const (
	MESSAGING_URL_PATH = "/messaging/rest/v1/events"
)

type MessageEventHandler struct {
	schConfig                        Config
	buildInfo                        *common.BuildInfo
	runtimeInfo                      *common.RuntimeInfo
	manager                          manager.Manager
	pipelineStoreTask                store.PipelineStoreTask
	quitSendingEventToDPM            chan bool
	ackEventList                     []*ClientEvent
	sendingPipelineStatusElapsedTime time.Time
}

func (m *MessageEventHandler) Init() {
	if m.schConfig.Enabled && m.schConfig.AppAuthToken != "" {
		ticker := time.NewTicker(time.Duration(m.schConfig.PingFrequency) * time.Millisecond)
		m.quitSendingEventToDPM = make(chan bool)
		go func() {
			err := m.SendEvent(true)
			if err != nil {
				log.WithError(err).Error()
			}
			for {
				select {
				case <-ticker.C:
					err := m.SendEvent(false)
					if err != nil {
						log.WithError(err).Error()
					}
				case <-m.quitSendingEventToDPM:
					ticker.Stop()
					return
				}
			}
		}()
	}
}

func (m *MessageEventHandler) SendEvent(sendInfoEvent bool) error {
	clientEventList := make([]*ClientEvent, 0)
	for _, ackEvent := range m.ackEventList {
		clientEventList = append(clientEventList, ackEvent)
	}

	if sendInfoEvent {
		clientEventList = append(clientEventList, m.createSdcEdgeInfoEvent())
	}

	if m.sendingPipelineStatusElapsedTime.IsZero() ||
		time.Since(m.sendingPipelineStatusElapsedTime).Seconds()*1e3 > float64(m.schConfig.StatusEventsInterval) {
		log.Debug("Send Pipeline Status Event")

		pipelineInfoList, err := m.pipelineStoreTask.GetPipelines()
		if err != nil {
			log.Println(err)
			return err
		}

		pipelineStatusEventList := make([]*PipelineStatusEvent, 0)
		for _, pipelineInfo := range pipelineInfoList {
			var offsetString string
			runner := m.manager.GetRunner(pipelineInfo.PipelineId)
			if runner != nil {
				sourceOffset, err := runner.GetOffset()
				if err != nil {
					log.WithError(err).Error()
					return err
				}
				offsetJson, err := json.Marshal(sourceOffset)
				if err != nil {
					log.WithError(err).Error()
					return err
				}

				offsetString = string(offsetJson)

				pipelineState, err := runner.GetStatus()
				if err != nil {
					log.Println(err)
					return err
				}

				if pipelineState.Status != common.EDITED {
					pipelineStatusEventList = append(
						pipelineStatusEventList,
						m.createPipelineStatusEvent(pipelineState, offsetString, runner.IsRemotePipeline()),
					)
				}
			}
		}
		pipelineStatusEvents := &PipelineStatusEvents{
			PipelineStatusEventList: pipelineStatusEventList,
		}
		pipelineStatusEventListJson, _ := json.Marshal(pipelineStatusEvents)
		pipelineStatusEvent := &ClientEvent{
			EventId:      uuid.NewV4().String(),
			Destinations: []string{m.schConfig.EventsRecipient},
			RequiresAck:  false,
			IsAckEvent:   false,
			EventTypeId:  STATUS_MULTIPLE_PIPELINES,
			Payload:      string(pipelineStatusEventListJson),
		}
		clientEventList = append(clientEventList, pipelineStatusEvent)

		m.sendingPipelineStatusElapsedTime = time.Now()
	}

	jsonValue, err := json.Marshal(clientEventList)
	if err != nil {
		log.WithError(err).Error()
		return err
	}

	var eventsUrl = m.schConfig.BaseUrl + MESSAGING_URL_PATH
	req, err := http.NewRequest("POST", eventsUrl, bytes.NewBuffer(jsonValue))
	req.Header.Set(common.HEADER_X_APP_AUTH_TOKEN, m.schConfig.AppAuthToken)
	req.Header.Set(common.HEADER_X_APP_COMPONENT_ID, m.runtimeInfo.ID)
	req.Header.Set(common.HEADER_X_REST_CALL, "true")
	req.Header.Set(common.HEADER_CONTENT_TYPE, common.APPLICATION_JSON)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	log.WithField("status", resp.Status).Debug("DPM Event Status")
	if resp.StatusCode != 200 {
		return errors.New("DPM Send event failed")
	}

	decoder := json.NewDecoder(resp.Body)
	var serverEventList []ServerEvent
	err = decoder.Decode(&serverEventList)
	if err != nil {
		switch {
		case err == io.EOF:
			// empty body
		case err != nil:
			// other error
			return errors.New(fmt.Sprintf("Parsing DPM event failed: %s", err))
		}
	}

	defer resp.Body.Close()

	ackClientEventList := make([]*ClientEvent, 0)
	for _, serverEvent := range serverEventList {
		ackEvent := m.handleDPMEvent(serverEvent)
		if ackEvent != nil {
			ackClientEventList = append(ackClientEventList, ackEvent)
		}
	}

	m.ackEventList = ackClientEventList

	return nil
}

func (m *MessageEventHandler) createSdcEdgeInfoEvent() *ClientEvent {
	sdcInfoEvent := SDCInfoEvent{
		EdgeId:        m.runtimeInfo.ID,
		HttpUrl:       m.runtimeInfo.HttpUrl,
		GoVersion:     runtime.Version(),
		EdgeBuildInfo: m.buildInfo,
		Labels:        m.schConfig.JobLabels,
		Edge:          true,
	}

	sdcInfoEventJson, _ := json.Marshal(sdcInfoEvent)

	sdcEdgeInfoEvent := &ClientEvent{
		EventId:      m.runtimeInfo.HttpUrl,
		Destinations: []string{m.schConfig.EventsRecipient},
		RequiresAck:  false,
		IsAckEvent:   false,
		EventTypeId:  SDC_INFO_EVENT,
		Payload:      string(sdcInfoEventJson),
	}

	return sdcEdgeInfoEvent
}

func (m *MessageEventHandler) createPipelineStatusEvent(
	pipelineState *common.PipelineState,
	offsetString string,
	isRemote bool,
) *PipelineStatusEvent {
	pipelineStatusEvent := &PipelineStatusEvent{
		Name:           pipelineState.PipelineId,
		Title:          pipelineState.PipelineId,
		TimeStamp:      pipelineState.TimeStamp.UnixNano() / int64(time.Millisecond),
		IsRemote:       isRemote,
		PipelineStatus: pipelineState.Status,
		Message:        pipelineState.Message,
		Offset:         offsetString,
	}
	return pipelineStatusEvent
}

func (m *MessageEventHandler) handleDPMEvent(serverEvent ServerEvent) *ClientEvent {
	log.Debug("Handling DPM Events: %d", serverEvent.EventTypeId)

	var ackEventMessage string
	ackEventStatus := ACK_EVENT_SUCCESS

	switch serverEvent.EventTypeId {
	case SAVE_PIPELINE:
		var pipelineSaveEvent PipelineSaveEvent
		if err := json.Unmarshal([]byte(serverEvent.Payload), &pipelineSaveEvent); err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.WithError(err).Error("Error during handling DPM SAVE Pipeline Event")
			break
		}

		var pipelineConfiguration common.PipelineConfiguration
		if err := json.Unmarshal([]byte(pipelineSaveEvent.PipelineConfigurationAndRules.PipelineConfig),
			&pipelineConfiguration); err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.WithError(err).Error("Error during handling DPM SAVE Pipeline Event")
			break
		}

		newPipeline, err := m.pipelineStoreTask.Create(
			pipelineSaveEvent.Name,
			pipelineConfiguration.Title,
			pipelineConfiguration.Description,
			true,
		)
		if err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.WithError(err).Error("Error during handling DPM SAVE Pipeline Event")
			break
		}

		pipelineConfiguration.UUID = newPipeline.UUID
		pipelineConfiguration.PipelineId = newPipeline.PipelineId
		_, err = m.pipelineStoreTask.Save(pipelineSaveEvent.Name, pipelineConfiguration)
		if err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.WithError(err).Error("Error during handling DPM SAVE Pipeline Event")
			break
		}

		// Update offset
		runner := m.manager.GetRunner(pipelineSaveEvent.Name)
		if runner != nil && len(pipelineSaveEvent.Offset) > 0 {
			log.Debug("Updating offset:", pipelineSaveEvent.Offset)

			var sourceOffset common.SourceOffset
			err := json.Unmarshal([]byte(pipelineSaveEvent.Offset), &sourceOffset)
			if err != nil {
				log.WithError(err).Error("Error de-serializing offset")
			} else {
				err = runner.CommitOffset(sourceOffset)
				if err != nil {
					log.WithError(err).Error("Error updating offset")
				}
			}
		}
	case START_PIPELINE:
		var pipelineBaseEvent PipelineBaseEvent
		if err := json.Unmarshal([]byte(serverEvent.Payload), &pipelineBaseEvent); err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.WithError(err).Error("Error handling DPM Start Pipeline Event")
			break
		}

		_, err := m.manager.StartPipeline(pipelineBaseEvent.Name, nil)
		if err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.WithError(err).Error("Error handling DPM Start Pipeline Event")
			break
		}
	case STOP_PIPELINE:
		var pipelineBaseEvent PipelineBaseEvent
		if err := json.Unmarshal([]byte(serverEvent.Payload), &pipelineBaseEvent); err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.WithError(err).Error("Error handling DPM Stop Pipeline Event")
			break
		}

		_, err := m.manager.StopPipeline(pipelineBaseEvent.Name)
		if err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.WithError(err).Error("Error handling DPM Stop Pipeline Event")
			break
		}
	case VALIDATE_PIPELINE:
	case RESET_OFFSET_PIPELINE:
	case DELETE_HISTORY_PIPELINE:
	case DELETE_PIPELINE:
		var pipelineBaseEvent PipelineBaseEvent
		if err := json.Unmarshal([]byte(serverEvent.Payload), &pipelineBaseEvent); err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.WithError(err).Error("Error handling DPM Delete Pipeline Event")
			break
		}

		err := m.pipelineStoreTask.Delete(pipelineBaseEvent.Name)
		if err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.WithError(err).Error("Error handling DPM Delete Pipeline Event")
			break
		}
	case STOP_DELETE_PIPELINE:
		var pipelineBaseEvent PipelineBaseEvent
		if err := json.Unmarshal([]byte(serverEvent.Payload), &pipelineBaseEvent); err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.WithError(err).Error("Error handling DPM Start Pipeline Event")
			break
		}

		_, err := m.manager.StopPipeline(pipelineBaseEvent.Name)
		if err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.WithError(err).Error("Error handling DPM Stop Delete Pipeline Event")
			break
		}

		err = m.pipelineStoreTask.Delete(pipelineBaseEvent.Name)
		if err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.WithError(err).Error("Error handling DPM Stop Delete Pipeline Event")
			break
		}
	default:
		ackEventMessage = fmt.Sprintf("Unrecognized event: %d", serverEvent.EventTypeId)
	}

	var ackClientEvent *ClientEvent
	if serverEvent.RequiresAck {
		ackEvent := &AckEvent{
			AckEventStatus: ackEventStatus,
			Message:        ackEventMessage,
		}
		ackEventJson, _ := json.Marshal(ackEvent)

		ackClientEvent = &ClientEvent{
			EventId:      serverEvent.EventId,
			Destinations: []string{m.schConfig.EventsRecipient},
			RequiresAck:  false,
			IsAckEvent:   true,
			EventTypeId:  ACK_EVENT,
			Payload:      string(ackEventJson),
		}
	}

	return ackClientEvent
}

func (m *MessageEventHandler) Shutdown() {
	m.quitSendingEventToDPM <- true
}

func NewMessageEventHandler(
	schConfig Config,
	buildInfo *common.BuildInfo,
	runtimeInfo *common.RuntimeInfo,
	pipelineStoreTask store.PipelineStoreTask,
	manager manager.Manager,
) *MessageEventHandler {
	messagingEventHandler := &MessageEventHandler{
		schConfig:         schConfig,
		buildInfo:         buildInfo,
		runtimeInfo:       runtimeInfo,
		manager:           manager,
		pipelineStoreTask: pipelineStoreTask,
	}
	return messagingEventHandler
}
