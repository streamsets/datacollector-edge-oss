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
package controlhub

import "github.com/streamsets/datacollector-edge/container/common"

const (
	VALIDATE_PIPELINE                 = 1000
	SAVE_PIPELINE                     = 1001
	SAVE_RULES_PIPELINE               = 1002
	START_PIPELINE                    = 1003
	STOP_PIPELINE                     = 1004
	RESET_OFFSET_PIPELINE             = 1005
	DELETE_PIPELINE                   = 1006
	DELETE_HISTORY_PIPELINE           = 1007
	PING_FREQUENCY_ADJUSTMENT         = 1008
	STOP_DELETE_PIPELINE              = 1009
	SSO_DISCONNECTED_MODE_CREDENTIALS = 1010
	SYNC_ACL                          = 1011
	STATUS_PIPELINE                   = 2000
	SDC_INFO_EVENT                    = 2001
	STATUS_MULTIPLE_PIPELINES         = 2002
	SDC_PROCESS_METRICS_EVENT         = 2003
	ACK_EVENT                         = 5000

	ACK_EVENT_SUCCESS = "SUCCESS"
	ACK_EVENT_ERROR   = "ERROR"
	ACK_EVENT_IGNORE  = "IGNORE"
)

type ClientEvent struct {
	EventId      string   `json:"eventId"`
	Destinations []string `json:"destinations"`
	RequiresAck  bool     `json:"requiresAck"`
	IsAckEvent   bool     `json:"ackEvent"`
	EventTypeId  int      `json:"eventTypeId"`
	Payload      string   `json:"payload"`
	OrgId        string   `json:"orgId"`
}

type ServerEvent struct {
	EventId      string `json:"eventId"`
	From         string `json:"from"`
	RequiresAck  bool   `json:"requiresAck"`
	IsAckEvent   bool   `json:"isAckEvent"`
	EventTypeId  int    `json:"eventTypeId"`
	Payload      string `json:"payload"`
	ReceivedTime int64  `json:"receivedTime"`
	OrgId        string `json:"orgId"`
}

type AckEvent struct {
	AckEventStatus string `json:"ackEventStatus"`
	Message        string `json:"message"`
}

type SDCInfoEvent struct {
	EdgeId        string            `json:"sdcId"`
	HttpUrl       string            `json:"httpUrl"`
	GoVersion     string            `json:"javaVersion"`
	EdgeBuildInfo *common.BuildInfo `json:"sdcBuildInfo"`
	Labels        []string          `json:"labels"`
	Edge          bool              `json:"edge"`
	TotalMemory   uint64            `json:"totalMemory"`
}

type SDCProcessMetricsEvent struct {
	Timestamp  int64   `json:"timestamp"`
	SdcId      string  `json:"sdcId"`
	CpuLoad    float64 `json:"cpuLoad"`
	UsedMemory uint64  `json:"usedMemory"`
}

type PipelineBaseEvent struct {
	Name string `json:"name"`
	Rev  string `json:"rev"`
	User string `json:"user"`
}

type PipelineSaveEvent struct {
	Name                          string                        `json:"name"`
	Rev                           string                        `json:"rev"`
	User                          string                        `json:"user"`
	PipelineConfigurationAndRules PipelineConfigurationAndRules `json:"pipelineConfigurationAndRules"`
	Description                   string                        `json:"description"`
	Offset                        string                        `json:"offset"`
	OffsetProtocolVersion         float64                       `json:"offsetProtocolVersion"`
	Acl                           interface{}                   `json:"acl"`
}

type PipelineConfigurationAndRules struct {
	PipelineConfig string `json:"pipelineConfig"`
	PipelineRules  string `json:"pipelineRules"`
}

type PipelineStatusEvent struct {
	Name                  string      `json:"name"`
	Title                 string      `json:"title"`
	Rev                   string      `json:"rev"`
	TimeStamp             int64       `json:"timeStamp"`
	IsRemote              bool        `json:"remote"`
	PipelineStatus        string      `json:"pipelineStatus"`
	Message               string      `json:"message"`
	WorkerInfos           interface{} `json:"workerInfos"`
	ValidationStatus      interface{} `json:"validationStatus"`
	Issues                string      `json:"issues"`
	IsClusterMode         bool        `json:"clusterMode"`
	Offset                string      `json:"offset"`
	OffsetProtocolVersion float64     `json:"offsetProtocolVersion"`
	Acl                   interface{} `json:"acl"`
	RunnerCount           float64     `json:"runnerCount"`
}

type PipelineStatusEvents struct {
	PipelineStatusEventList []*PipelineStatusEvent `json:"pipelineStatusEventList"`
}
