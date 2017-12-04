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
	"errors"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/execution"
	"github.com/streamsets/datacollector-edge/container/execution/store"
	pipelineStore "github.com/streamsets/datacollector-edge/container/store"
	"github.com/streamsets/datacollector-edge/container/util"
	"time"
)

var (
	RESET_OFFSET_DISALLOWED_STATUSES = []string{
		common.FINISHING,
		common.RETRY,
		common.RUNNING,
		common.STARTING,
		common.STOPPING,
	}

	UPDATE_OFFSET_ALLOWED_STATUSES = []string{
		common.EDITED,
		common.FINISHED,
		common.STOPPED,
	}
)

type StandaloneRunner struct {
	runtimeInfo          *common.RuntimeInfo
	pipelineId           string
	config               execution.Config
	validTransitions     map[string][]string
	pipelineState        *common.PipelineState
	pipelineConfig       common.PipelineConfiguration
	prodPipeline         *ProductionPipeline
	metricsEventRunnable *MetricsEventRunnable
	pipelineStoreTask    pipelineStore.PipelineStoreTask
}

func (standaloneRunner *StandaloneRunner) init() error {
	standaloneRunner.validTransitions = make(map[string][]string)
	standaloneRunner.validTransitions[common.EDITED] = []string{common.STARTING}
	standaloneRunner.validTransitions[common.STARTING] = []string{common.START_ERROR, common.RUNNING, common.STOPPING}
	standaloneRunner.validTransitions[common.START_ERROR] = []string{common.STARTING}
	standaloneRunner.validTransitions[common.RUNNING] = []string{common.RUNNING_ERROR, common.FINISHING, common.STOPPING}
	standaloneRunner.validTransitions[common.RUNNING_ERROR] = []string{common.RETRY, common.RUN_ERROR}
	standaloneRunner.validTransitions[common.RETRY] = []string{common.STARTING, common.STOPPING}
	standaloneRunner.validTransitions[common.RUN_ERROR] = []string{common.STARTING}
	standaloneRunner.validTransitions[common.FINISHING] = []string{common.FINISHED}
	standaloneRunner.validTransitions[common.STOPPING] = []string{common.STOPPED}
	standaloneRunner.validTransitions[common.FINISHED] = []string{common.STARTING}
	standaloneRunner.validTransitions[common.STOPPED] = []string{common.STARTING}

	var err error
	standaloneRunner.pipelineState, err = store.GetState(standaloneRunner.pipelineId)
	return err
}

func (standaloneRunner *StandaloneRunner) GetPipelineConfig() common.PipelineConfiguration {
	return standaloneRunner.pipelineConfig
}

func (standaloneRunner *StandaloneRunner) GetStatus() (*common.PipelineState, error) {
	return standaloneRunner.pipelineState, nil
}

func (standaloneRunner *StandaloneRunner) GetHistory() ([]*common.PipelineState, error) {
	return store.GetHistory(standaloneRunner.pipelineId)
}

func (standaloneRunner *StandaloneRunner) GetMetrics() (metrics.Registry, error) {
	if standaloneRunner.prodPipeline != nil {
		return standaloneRunner.prodPipeline.MetricRegistry, nil
	}
	return nil, errors.New("Pipeline is not running")
}

func (standaloneRunner *StandaloneRunner) StartPipeline(
	runtimeParameters map[string]interface{},
) (*common.PipelineState, error) {
	log.WithField("id", standaloneRunner.pipelineId).Info("Starting pipeline")
	var err error
	err = standaloneRunner.checkState(common.STARTING)
	if err != nil {
		return nil, err
	}

	standaloneRunner.pipelineConfig, err = standaloneRunner.pipelineStoreTask.LoadPipelineConfig(
		standaloneRunner.pipelineId,
	)
	if err != nil {
		return nil, err
	}

	if standaloneRunner.prodPipeline, err = NewProductionPipeline(
		standaloneRunner.pipelineId,
		standaloneRunner.config,
		standaloneRunner,
		standaloneRunner.pipelineConfig,
		runtimeParameters,
	); err != nil {
		return nil, err
	}

	go standaloneRunner.prodPipeline.Run()

	if standaloneRunner.runtimeInfo.DPMEnabled && standaloneRunner.IsRemotePipeline() {
		standaloneRunner.metricsEventRunnable = NewMetricsEventRunnable(
			standaloneRunner.pipelineId,
			standaloneRunner.pipelineConfig,
			standaloneRunner.prodPipeline.Pipeline.pipelineBean,
			standaloneRunner.prodPipeline.MetricRegistry,
			standaloneRunner.runtimeInfo,
		)
		go standaloneRunner.metricsEventRunnable.Run()
	}

	standaloneRunner.pipelineState.Status = common.RUNNING
	standaloneRunner.pipelineState.TimeStamp = time.Now().UTC()

	if err = store.SaveState(standaloneRunner.pipelineId, standaloneRunner.pipelineState); err != nil {
		return nil, err
	}

	return standaloneRunner.pipelineState, nil
}

func (standaloneRunner *StandaloneRunner) StopPipeline() (*common.PipelineState, error) {
	log.WithField("id", standaloneRunner.pipelineId).Info("Stopping pipeline")
	var err error
	err = standaloneRunner.checkState(common.STOPPING)
	if err != nil {
		return nil, err
	}

	if standaloneRunner.prodPipeline != nil {
		standaloneRunner.prodPipeline.Stop()
	}

	if standaloneRunner.metricsEventRunnable != nil {
		standaloneRunner.metricsEventRunnable.Stop()
	}

	standaloneRunner.pipelineState.Status = common.STOPPED
	standaloneRunner.pipelineState.TimeStamp = time.Now().UTC()
	err = store.SaveState(standaloneRunner.pipelineId, standaloneRunner.pipelineState)
	if err != nil {
		return nil, err
	}

	return standaloneRunner.pipelineState, nil
}

func (standaloneRunner *StandaloneRunner) ResetOffset() error {
	if util.Contains(RESET_OFFSET_DISALLOWED_STATUSES, standaloneRunner.pipelineState.Status) {
		return errors.New("Cannot reset the source offset when the pipeline is running")
	}
	err := store.ResetOffset(standaloneRunner.pipelineId)
	return err
}

func (standaloneRunner *StandaloneRunner) CommitOffset(sourceOffset common.SourceOffset) error {
	if util.Contains(UPDATE_OFFSET_ALLOWED_STATUSES, standaloneRunner.pipelineState.Status) {
		return store.SaveOffset(standaloneRunner.pipelineId, sourceOffset)
	} else {
		return errors.New("Cannot update the source offset when the pipeline is running")
	}
}

func (standaloneRunner *StandaloneRunner) GetOffset() (common.SourceOffset, error) {
	return store.GetOffset(standaloneRunner.pipelineId)
}

func (standaloneRunner *StandaloneRunner) checkState(toState string) error {
	supportedList := standaloneRunner.validTransitions[standaloneRunner.pipelineState.Status]
	if !util.Contains(supportedList, toState) {
		return errors.New("Cannot change state from " + standaloneRunner.pipelineState.Status +
			" to " + toState)
	}
	return nil
}

func (standaloneRunner *StandaloneRunner) IsRemotePipeline() bool {
	attributes := standaloneRunner.pipelineState.Attributes
	return attributes != nil && attributes[store.IS_REMOTE_PIPELINE] == true
}

func NewStandaloneRunner(
	pipelineId string,
	config execution.Config,
	runtimeInfo *common.RuntimeInfo,
	pipelineStoreTask pipelineStore.PipelineStoreTask,
) (*StandaloneRunner, error) {
	standaloneRunner := StandaloneRunner{
		pipelineId:        pipelineId,
		config:            config,
		runtimeInfo:       runtimeInfo,
		pipelineStoreTask: pipelineStoreTask,
	}
	store.BaseDir = runtimeInfo.BaseDir
	err := standaloneRunner.init()
	return &standaloneRunner, err
}
