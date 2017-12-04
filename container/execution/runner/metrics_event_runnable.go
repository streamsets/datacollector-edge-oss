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
package runner

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/util"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	STATS_DPM_DIRECTLY_TARGET = "com_streamsets_pipeline_stage_destination_devnull_StatsDpmDirectlyDTarget"
	REMOTE_TIMESERIES_URL     = "REMOTE_TIMESERIES_URL"
	PIPELINE_COMMIT_ID        = "PIPELINE_COMMIT_ID"
	JOB_ID                    = "JOB_ID"
	UPDATE_WAIT_TIME_MS       = "UPDATE_WAIT_TIME_MS"
	DPM_PIPELINE_COMMIT_ID    = "dpm.pipeline.commitId"
	DPM_JOB_ID                = "dpm.job.id"
)

type MetricsEventRunnable struct {
	pipelineId              string
	pipelineConfig          common.PipelineConfiguration
	pipelineBean            creation.PipelineBean
	metricRegistry          metrics.Registry
	runtimeInfo             *common.RuntimeInfo
	quitSendingMetricsToDPM chan bool
	remoteTimeSeriesUrl     string
	pipelineCommitId        string
	jobId                   string
	waitTimeBetweenUpdates  int64
	metadata                map[string]string
}

type SDCMetrics struct {
	Timestamp   int64             `json:"timestamp"`
	Metadata    map[string]string `json:"metadata"`
	SdcId       string            `json:"sdcId"`
	Aggregated  bool              `json:"aggregated"`
	MasterSdcId string            `json:"masterSdcId"`
	Metrics     util.MetricsJson  `json:"metrics"`
}

func (m *MetricsEventRunnable) Run() {
	if m.isWriteStatsToDPMDirectlyEnabled() {
		m.initializeDPMMetricsVariables()
		ticker := time.NewTicker(time.Duration(m.waitTimeBetweenUpdates) * time.Millisecond)
		m.quitSendingMetricsToDPM = make(chan bool)
		for {
			select {
			case <-ticker.C:
				err := m.sendMetricsToDPM()
				if err != nil {
					log.WithError(err).Error()
				}
			case <-m.quitSendingMetricsToDPM:
				ticker.Stop()
				log.Debug("Sending metrics to DPM is stopped")
				return
			}
		}
	}
}

func (m *MetricsEventRunnable) Stop() {
	if m.isWriteStatsToDPMDirectlyEnabled() {
		m.quitSendingMetricsToDPM <- true
	}
}

func (m *MetricsEventRunnable) sendMetricsToDPM() error {
	log.Debug("Sending metrics to DPM")
	metricsJson := SDCMetrics{
		Timestamp:   time.Now().UnixNano() / int64(time.Millisecond),
		Metadata:    m.metadata,
		SdcId:       m.runtimeInfo.ID,
		Aggregated:  false,
		MasterSdcId: "",
		Metrics:     util.FormatMetricsRegistry(m.metricRegistry),
	}

	jsonValue, err := json.Marshal([]SDCMetrics{metricsJson})
	if err != nil {
		log.Println(err)
		return err
	}

	req, err := http.NewRequest(common.HTTP_POST, m.remoteTimeSeriesUrl, bytes.NewBuffer(jsonValue))
	req.Header.Set(common.HEADER_X_APP_AUTH_TOKEN, m.runtimeInfo.AppAuthToken)
	req.Header.Set(common.HEADER_X_APP_COMPONENT_ID, m.runtimeInfo.ID)
	req.Header.Set(common.HEADER_X_REST_CALL, common.HEADER_X_REST_CALL_VALUE)
	req.Header.Set(common.HEADER_CONTENT_TYPE, common.APPLICATION_JSON)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	log.WithField("status", resp.Status).Debug("DPM Send Metrics Status")
	if resp.StatusCode != 200 {
		responseData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return errors.New(fmt.Sprintf("DPM Send Metrics failed - %s ", string(responseData)))
	}

	return nil
}

func (m *MetricsEventRunnable) isWriteStatsToDPMDirectlyEnabled() bool {
	statsAggregatorStage := m.pipelineConfig.StatsAggregatorStage
	if len(statsAggregatorStage.StageName) > 0 || statsAggregatorStage.StageName == STATS_DPM_DIRECTLY_TARGET {
		return true
	}
	return false
}

func (m *MetricsEventRunnable) initializeDPMMetricsVariables() {
	for k, v := range m.pipelineBean.Config.Constants {
		switch k {
		case REMOTE_TIMESERIES_URL:
			m.remoteTimeSeriesUrl = v.(string)
		case PIPELINE_COMMIT_ID:
			m.pipelineCommitId = v.(string)
		case JOB_ID:
			m.jobId = v.(string)
		case UPDATE_WAIT_TIME_MS:
			m.waitTimeBetweenUpdates = int64(v.(float64))
		}
	}

	m.metadata = make(map[string]string)
	m.metadata[DPM_PIPELINE_COMMIT_ID] = m.pipelineCommitId
	m.metadata[DPM_JOB_ID] = m.jobId
	for k, v := range m.pipelineConfig.Metadata {
		switch v.(type) {
		case string:
			m.metadata[k] = v.(string)
		}
	}
}

func NewMetricsEventRunnable(
	pipelineId string,
	pipelineConfig common.PipelineConfiguration,
	pipelineBean creation.PipelineBean,
	metricRegistry metrics.Registry,
	runtimeInfo *common.RuntimeInfo,
) *MetricsEventRunnable {
	return &MetricsEventRunnable{
		pipelineId:              pipelineId,
		pipelineConfig:          pipelineConfig,
		pipelineBean:            pipelineBean,
		metricRegistry:          metricRegistry,
		runtimeInfo:             runtimeInfo,
		quitSendingMetricsToDPM: make(chan bool),
	}
}
