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
	"strconv"
	"time"
)

const (
	STATS_DPM_DIRECTLY_TARGET        = "com_streamsets_pipeline_stage_destination_devnull_StatsDpmDirectlyDTarget"
	REMOTE_TIMESERIES_URL            = "REMOTE_TIMESERIES_URL"
	PIPELINE_COMMIT_ID               = "PIPELINE_COMMIT_ID"
	JOB_ID                           = "JOB_ID"
	UPDATE_WAIT_TIME_MS              = "UPDATE_WAIT_TIME_MS"
	DPM_PIPELINE_COMMIT_ID           = "dpm.pipeline.commitId"
	DPM_JOB_ID                       = "dpm.job.id"
	TIME_SERIES_ANALYSIS_PARAM_ID    = "TIME_SERIES_ANALYSIS"
	TIME_SERIES_ANALYSIS_METADATA_ID = "timeSeriesAnalysis"
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
	timeSeriesAnalysis      bool
	metadata                map[string]string
	httpClient              *http.Client
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
				// send latest metrics to control hub before stopping
				err := m.sendMetricsToDPM()
				if err != nil {
					log.WithError(err).Error()
				}
				log.Debug("Sending metrics to Control Hub is stopped")
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
	log.Debug("Sending metrics to Control Hub")
	metricsJson := SDCMetrics{
		Timestamp:   util.ConvertTimeToLong(time.Now()),
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

	req, err := http.NewRequest(common.HttpPost, m.remoteTimeSeriesUrl, bytes.NewBuffer(jsonValue))
	req.Header.Set(common.HeaderXAppAuthToken, m.runtimeInfo.AppAuthToken)
	req.Header.Set(common.HeaderXAppComponentId, m.runtimeInfo.ID)
	req.Header.Set(common.HeaderXRestCall, common.HeaderXRestCallValue)
	req.Header.Set(common.HeaderContentType, common.ApplicationJson)

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.WithError(err).Error("Error while closing the response body")
		}
	}()

	log.WithField("status", resp.Status).Debug("Control Hub Send Metrics Status")
	if resp.StatusCode != 200 {
		responseData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return errors.New(fmt.Sprintf("Control Hub Send Metrics failed - %s ", string(responseData)))
	}

	return nil
}

func (m *MetricsEventRunnable) isWriteStatsToDPMDirectlyEnabled() bool {
	statsAggregatorStage := m.pipelineConfig.StatsAggregatorStage
	if statsAggregatorStage != nil && statsAggregatorStage.StageName == STATS_DPM_DIRECTLY_TARGET {
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
		case TIME_SERIES_ANALYSIS_PARAM_ID:
			m.timeSeriesAnalysis = v.(bool)
		}
	}

	m.metadata = make(map[string]string)
	m.metadata[DPM_PIPELINE_COMMIT_ID] = m.pipelineCommitId
	m.metadata[DPM_JOB_ID] = m.jobId
	m.metadata[TIME_SERIES_ANALYSIS_METADATA_ID] = strconv.FormatBool(m.timeSeriesAnalysis)
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
		httpClient:              &http.Client{},
	}
}
