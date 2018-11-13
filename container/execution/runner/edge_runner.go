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
	"errors"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/execution"
	"github.com/streamsets/datacollector-edge/container/execution/store"
	pipelineStore "github.com/streamsets/datacollector-edge/container/store"
	"github.com/streamsets/datacollector-edge/container/util"
	"time"
)

var (
	RestOffsetDisallowedStatuses = []string{
		common.FINISHING,
		common.RETRY,
		common.RUNNING,
		common.STARTING,
		common.STOPPING,
	}
	UpdateOffsetAllowedStatuses = []string{
		common.EDITED,
		common.FINISHED,
		common.STOPPED,
	}
)

type EdgeRunner struct {
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

func (edgeRunner *EdgeRunner) init() error {
	edgeRunner.validTransitions = make(map[string][]string)
	edgeRunner.validTransitions[common.EDITED] = []string{common.STARTING}
	edgeRunner.validTransitions[common.STARTING] = []string{common.START_ERROR, common.RUNNING, common.STOPPING}
	edgeRunner.validTransitions[common.START_ERROR] = []string{common.STARTING}
	edgeRunner.validTransitions[common.RUNNING] = []string{common.RUNNING_ERROR, common.FINISHING, common.STOPPING}
	edgeRunner.validTransitions[common.RUNNING_ERROR] = []string{common.RETRY, common.RUN_ERROR}
	edgeRunner.validTransitions[common.RETRY] = []string{common.STARTING, common.STOPPING}
	edgeRunner.validTransitions[common.RUN_ERROR] = []string{common.STARTING}
	edgeRunner.validTransitions[common.FINISHING] = []string{common.FINISHED}
	edgeRunner.validTransitions[common.STOPPING] = []string{common.STOPPED}
	edgeRunner.validTransitions[common.FINISHED] = []string{common.STARTING}
	edgeRunner.validTransitions[common.STOPPED] = []string{common.STARTING}

	var err error
	edgeRunner.pipelineState, err = store.GetState(edgeRunner.pipelineId)
	return err
}

func (edgeRunner *EdgeRunner) GetPipelineConfig() common.PipelineConfiguration {
	return edgeRunner.pipelineConfig
}

func (edgeRunner *EdgeRunner) GetStatus() (*common.PipelineState, error) {
	return edgeRunner.pipelineState, nil
}

func (edgeRunner *EdgeRunner) GetHistory() ([]*common.PipelineState, error) {
	return store.GetHistory(edgeRunner.pipelineId)
}

func (edgeRunner *EdgeRunner) GetMetrics() (metrics.Registry, error) {
	if edgeRunner.prodPipeline != nil {
		return edgeRunner.prodPipeline.MetricRegistry, nil
	}
	return nil, errors.New("pipeline is not running")
}

func (edgeRunner *EdgeRunner) StartPipeline(
	runtimeParameters map[string]interface{},
) (*common.PipelineState, error) {
	log.WithField("id", edgeRunner.pipelineId).Info("Starting pipeline")
	var err error
	err = edgeRunner.checkState(common.STARTING)
	if err != nil {
		return nil, err
	}

	edgeRunner.pipelineConfig, err = edgeRunner.pipelineStoreTask.LoadPipelineConfig(
		edgeRunner.pipelineId,
	)
	if err != nil {
		return nil, err
	}

	var issues []validation.Issue
	if edgeRunner.prodPipeline, issues = NewProductionPipeline(
		edgeRunner.pipelineId,
		edgeRunner.config,
		edgeRunner,
		edgeRunner.pipelineConfig,
		runtimeParameters,
	); err != nil {
		return nil, err
	}

	if len(issues) != 0 {
		return edgeRunner.setStateToStartError(issues)
	}

	issues = edgeRunner.prodPipeline.Init()

	if len(issues) != 0 {
		return edgeRunner.setStateToStartError(issues)
	}

	go func() {
		edgeRunner.prodPipeline.Run()
		if edgeRunner.prodPipeline.Pipeline.offsetTracker.IsFinished() {
			edgeRunner.pipelineState.Status = common.FINISHED
			edgeRunner.pipelineState.TimeStamp = util.ConvertTimeToLong(time.Now())
			err = store.SaveState(edgeRunner.pipelineId, edgeRunner.pipelineState)
			if err != nil {
				log.WithError(err).Error("Failed to save pipeline state to finished")
			}
		}
	}()

	if edgeRunner.runtimeInfo.DPMEnabled && edgeRunner.IsRemotePipeline() {
		edgeRunner.metricsEventRunnable = NewMetricsEventRunnable(
			edgeRunner.pipelineId,
			edgeRunner.pipelineConfig,
			edgeRunner.prodPipeline.Pipeline.pipelineBean,
			edgeRunner.prodPipeline.MetricRegistry,
			edgeRunner.runtimeInfo,
		)
		go edgeRunner.metricsEventRunnable.Run()
	}

	edgeRunner.pipelineState.Status = common.RUNNING
	edgeRunner.pipelineState.TimeStamp = util.ConvertTimeToLong(time.Now())

	if err = store.SaveState(edgeRunner.pipelineId, edgeRunner.pipelineState); err != nil {
		return nil, err
	}

	return edgeRunner.pipelineState, nil
}

func (edgeRunner *EdgeRunner) setStateToStartError(issues []validation.Issue) (*common.PipelineState, error) {
	edgeRunner.pipelineState.Status = common.START_ERROR
	edgeRunner.pipelineState.TimeStamp = util.ConvertTimeToLong(time.Now())
	edgeRunner.pipelineState.Message = issues[0].Message
	edgeRunner.pipelineState.Attributes[store.ISSUES] = validation.NewIssues(issues)
	if err := store.SaveState(edgeRunner.pipelineId, edgeRunner.pipelineState); err != nil {
		return nil, err
	}
	return edgeRunner.pipelineState, nil
}

func (edgeRunner *EdgeRunner) StopPipeline() (*common.PipelineState, error) {
	log.WithField("id", edgeRunner.pipelineId).Info("Stopping pipeline")
	var err error
	err = edgeRunner.checkState(common.STOPPING)
	if err != nil {
		return nil, err
	}

	if edgeRunner.prodPipeline != nil {
		edgeRunner.prodPipeline.Stop()
	}

	if edgeRunner.metricsEventRunnable != nil {
		edgeRunner.metricsEventRunnable.Stop()
	}

	edgeRunner.pipelineState.Status = common.STOPPED
	edgeRunner.pipelineState.TimeStamp = util.ConvertTimeToLong(time.Now())
	err = store.SaveState(edgeRunner.pipelineId, edgeRunner.pipelineState)
	if err != nil {
		return nil, err
	}

	return edgeRunner.pipelineState, nil
}

func (edgeRunner *EdgeRunner) ResetOffset() error {
	if util.Contains(RestOffsetDisallowedStatuses, edgeRunner.pipelineState.Status) {
		return errors.New("cannot reset the source offset when the pipeline is running")
	}
	err := store.ResetOffset(edgeRunner.pipelineId)
	return err
}

func (edgeRunner *EdgeRunner) CommitOffset(sourceOffset common.SourceOffset) error {
	if util.Contains(UpdateOffsetAllowedStatuses, edgeRunner.pipelineState.Status) {
		return store.SaveOffset(edgeRunner.pipelineId, sourceOffset)
	} else {
		return errors.New("cannot update the source offset when the pipeline is running")
	}
}

func (edgeRunner *EdgeRunner) GetOffset() (common.SourceOffset, error) {
	return store.GetOffset(edgeRunner.pipelineId)
}

func (edgeRunner *EdgeRunner) checkState(toState string) error {
	supportedList := edgeRunner.validTransitions[edgeRunner.pipelineState.Status]
	if !util.Contains(supportedList, toState) {
		return errors.New("Cannot change state from " + edgeRunner.pipelineState.Status +
			" to " + toState)
	}
	return nil
}

func (edgeRunner *EdgeRunner) IsRemotePipeline() bool {
	attributes := edgeRunner.pipelineState.Attributes
	return attributes != nil && attributes[store.IS_REMOTE_PIPELINE] == true
}

func (edgeRunner *EdgeRunner) GetErrorRecords(stageInstanceName string, size int) ([]api.Record, error) {
	return edgeRunner.prodPipeline.Pipeline.GetErrorRecords(stageInstanceName, size)
}

func (edgeRunner *EdgeRunner) GetErrorMessages(stageInstanceName string, size int) ([]api.ErrorMessage, error) {
	return edgeRunner.prodPipeline.Pipeline.GetErrorMessages(stageInstanceName, size)
}

func NewEdgeRunner(
	pipelineId string,
	config execution.Config,
	runtimeInfo *common.RuntimeInfo,
	pipelineStoreTask pipelineStore.PipelineStoreTask,
) (execution.Runner, error) {
	edgeRunner := EdgeRunner{
		pipelineId:        pipelineId,
		config:            config,
		runtimeInfo:       runtimeInfo,
		pipelineStoreTask: pipelineStoreTask,
	}
	store.BaseDir = runtimeInfo.BaseDir
	err := edgeRunner.init()
	return &edgeRunner, err
}
