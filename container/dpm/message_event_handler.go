package dpm

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/execution/manager"
	"github.com/streamsets/datacollector-edge/container/store"
	"io"
	"log"
	"net/http"
	"runtime"
	"time"
)

const (
	MESSAGING_URL_PATH = "/messaging/rest/v1/events"
)

type MessageEventHandler struct {
	dpmConfig             Config
	buildInfo             *common.BuildInfo
	runtimeInfo           *common.RuntimeInfo
	manager               manager.Manager
	pipelineStoreTask     store.PipelineStoreTask
	quitSendingEventToDPM chan bool
	ackEventList          []*ClientEvent
}

func (m *MessageEventHandler) Init() {
	ticker := time.NewTicker(time.Duration(m.dpmConfig.PingFrequency) * time.Millisecond)
	m.quitSendingEventToDPM = make(chan bool)
	go func() {
		for {
			select {
			case <-ticker.C:
				err := m.SendEvent()
				if err != nil {
					log.Println("[ERROR] ", err)
				}
			case <-m.quitSendingEventToDPM:
				ticker.Stop()
				return
			}
		}
	}()
}

func (m *MessageEventHandler) SendEvent() error {
	if m.dpmConfig.Enabled && m.dpmConfig.AppAuthToken != "" {
		sdcInfoEvent := SDCInfoEvent{
			EdgeId:        m.runtimeInfo.ID,
			HttpUrl:       m.runtimeInfo.HttpUrl,
			GoVersion:     runtime.Version(),
			EdgeBuildInfo: m.buildInfo,
			Labels:        m.dpmConfig.JobLabels,
			Edge:          true,
		}

		sdcInfoEventJson, _ := json.Marshal(sdcInfoEvent)

		clientEventList := make([]*ClientEvent, 0)
		for _, ackEvent := range m.ackEventList {
			clientEventList = append(clientEventList, ackEvent)
		}

		sdcEdgeInfoEvent := &ClientEvent{
			EventId:      m.runtimeInfo.HttpUrl,
			Destinations: []string{m.dpmConfig.EventsRecipient},
			RequiresAck:  false,
			IsAckEvent:   false,
			EventTypeId:  SDC_INFO_EVENT,
			Payload:      string(sdcInfoEventJson),
		}

		clientEventList = append(clientEventList, sdcEdgeInfoEvent)

		jsonValue, err := json.Marshal(clientEventList)
		if err != nil {
			log.Println(err)
		}

		var eventsUrl = m.dpmConfig.BaseUrl + MESSAGING_URL_PATH

		req, err := http.NewRequest("POST", eventsUrl, bytes.NewBuffer(jsonValue))
		req.Header.Set(common.HEADER_X_APP_AUTH_TOKEN, m.dpmConfig.AppAuthToken)
		req.Header.Set(common.HEADER_X_APP_COMPONENT_ID, m.runtimeInfo.ID)
		req.Header.Set(common.HEADER_X_REST_CALL, "SDC Edge")
		req.Header.Set(common.HEADER_CONTENT_TYPE, common.APPLICATION_JSON)

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}

		log.Println("[DEBUG] DPM Event Status:", resp.Status)
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
	}

	return nil
}

func (m *MessageEventHandler) handleDPMEvent(serverEvent ServerEvent) *ClientEvent {
	log.Printf("[DEBUG] Handling DPM Events: %d", serverEvent.EventTypeId)

	var ackEventMessage string
	ackEventStatus := ACK_EVENT_SUCCESS

	switch serverEvent.EventTypeId {
	case SAVE_PIPELINE:
		var pipelineSaveEvent PipelineSaveEvent
		if err := json.Unmarshal([]byte(serverEvent.Payload), &pipelineSaveEvent); err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.Println("[Error] Error during handling DPM SAVE Pipeline Event:", err)
			break
		}

		var pipelineConfiguration common.PipelineConfiguration
		if err := json.Unmarshal([]byte(pipelineSaveEvent.PipelineConfigurationAndRules.PipelineConfig),
			&pipelineConfiguration); err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.Println("[Error] Error during handling DPM SAVE Pipeline Event:", err)
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
			log.Println("[Error] Error during handling DPM SAVE Pipeline Event:", err)
			break
		}

		pipelineConfiguration.UUID = newPipeline.UUID
		_, err = m.pipelineStoreTask.Save(pipelineSaveEvent.Name, pipelineConfiguration)
		if err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.Println("[Error] Error during handling DPM SAVE Pipeline Event:", err)
			break
		}
	case START_PIPELINE:
		var pipelineBaseEvent PipelineBaseEvent
		if err := json.Unmarshal([]byte(serverEvent.Payload), &pipelineBaseEvent); err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.Println("[Error] Error during handling DPM Start Pipeline Event:", err)
			break
		}

		_, err := m.manager.StartPipeline(pipelineBaseEvent.Name, nil)
		if err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.Println("[Error] Error during handling DPM Start Pipeline Event:", err)
			break
		}
	case STOP_PIPELINE:
		var pipelineBaseEvent PipelineBaseEvent
		if err := json.Unmarshal([]byte(serverEvent.Payload), &pipelineBaseEvent); err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.Println("[Error] Error during handling DPM Stop Pipeline Event:", err)
			break
		}

		_, err := m.manager.StopPipeline(pipelineBaseEvent.Name)
		if err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.Println("[Error] Error during handling DPM Stop Pipeline Event:", err)
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
			log.Println("[Error] Error during handling DPM Delete Pipeline Event:", err)
			break
		}

		err := m.pipelineStoreTask.Delete(pipelineBaseEvent.Name)
		if err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.Println("[Error] Error during handling DPM Delete Pipeline Event:", err)
			break
		}
	case STOP_DELETE_PIPELINE:
		var pipelineBaseEvent PipelineBaseEvent
		if err := json.Unmarshal([]byte(serverEvent.Payload), &pipelineBaseEvent); err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.Println("[Error] Error during handling DPM Start Pipeline Event:", err)
			break
		}

		_, err := m.manager.StopPipeline(pipelineBaseEvent.Name)
		if err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.Println("[Error] Error during handling DPM Stop Delete Pipeline Event:", err)
			break
		}

		err = m.pipelineStoreTask.Delete(pipelineBaseEvent.Name)
		if err != nil {
			ackEventMessage = err.Error()
			ackEventStatus = ACK_EVENT_ERROR
			log.Println("[Error] Error during handling DPM Stop Delete Pipeline Event:", err)
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
			Destinations: []string{m.dpmConfig.EventsRecipient},
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
	dpmConfig Config,
	buildInfo *common.BuildInfo,
	runtimeInfo *common.RuntimeInfo,
	pipelineStoreTask store.PipelineStoreTask,
	manager manager.Manager,
) *MessageEventHandler {
	messagingEventHandler := &MessageEventHandler{
		dpmConfig:         dpmConfig,
		buildInfo:         buildInfo,
		runtimeInfo:       runtimeInfo,
		manager:           manager,
		pipelineStoreTask: pipelineStoreTask,
	}
	return messagingEventHandler
}
