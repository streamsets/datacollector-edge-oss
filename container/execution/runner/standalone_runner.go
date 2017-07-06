package runner

import (
	"errors"
	"github.com/rcrowley/go-metrics"
	"github.com/streamsets/sdc2go/container/common"
	"github.com/streamsets/sdc2go/container/execution"
	"github.com/streamsets/sdc2go/container/execution/store"
	pipelineStore "github.com/streamsets/sdc2go/container/store"
	"github.com/streamsets/sdc2go/container/util"
	"time"
)

var RESET_OFFSET_DISALLOWED_STATUSES = []string{
	common.FINISHING,
	common.RETRY,
	common.RUNNING,
	common.STARTING,
	common.STOPPING,
}

type StandaloneRunner struct {
	runtimeInfo       common.RuntimeInfo
	pipelineId        string
	config            execution.Config
	validTransitions  map[string][]string
	pipelineState     *common.PipelineState
	pipelineConfig    common.PipelineConfiguration
	prodPipeline      *ProductionPipeline
	pipelineStoreTask pipelineStore.PipelineStoreTask
}

func (standaloneRunner *StandaloneRunner) init() {
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
	if err != nil {
		panic(err)
	}
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

	standaloneRunner.prodPipeline, err = NewProductionPipeline(
		standaloneRunner.pipelineId,
		standaloneRunner.config,
		standaloneRunner,
		standaloneRunner.pipelineConfig,
		runtimeParameters,
	)
	if err != nil {
		return nil, err
	}

	go standaloneRunner.prodPipeline.Run()

	standaloneRunner.pipelineState.Status = common.RUNNING
	standaloneRunner.pipelineState.TimeStamp = time.Now().UTC()
	err = store.SaveState(standaloneRunner.pipelineId, standaloneRunner.pipelineState)
	if err != nil {
		return nil, err
	}

	return standaloneRunner.pipelineState, nil
}

func (standaloneRunner *StandaloneRunner) StopPipeline() (*common.PipelineState, error) {
	var err error
	err = standaloneRunner.checkState(common.STOPPING)
	if err != nil {
		return nil, err
	}

	if standaloneRunner.prodPipeline != nil {
		standaloneRunner.prodPipeline.Stop()
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

func (standaloneRunner *StandaloneRunner) checkState(toState string) error {
	supportedList := standaloneRunner.validTransitions[standaloneRunner.pipelineState.Status]
	if !util.Contains(supportedList, toState) {
		return errors.New("Cannot change state from " + standaloneRunner.pipelineState.Status +
			" to " + toState)
	}
	return nil
}

func NewStandaloneRunner(
	pipelineId string,
	config execution.Config,
	runtimeInfo common.RuntimeInfo,
	pipelineStoreTask pipelineStore.PipelineStoreTask,
) (*StandaloneRunner, error) {
	standaloneRunner := StandaloneRunner{
		pipelineId:        pipelineId,
		config:            config,
		runtimeInfo:       runtimeInfo,
		pipelineStoreTask: pipelineStoreTask,
	}
	store.BaseDir = runtimeInfo.BaseDir
	standaloneRunner.init()
	return &standaloneRunner, nil
}
