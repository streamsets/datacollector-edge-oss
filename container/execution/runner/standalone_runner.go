package runner

import (
	"errors"
	"github.com/streamsets/dataextractor/container/common"
	"github.com/streamsets/dataextractor/container/creation"
	"github.com/streamsets/dataextractor/container/execution"
	"github.com/streamsets/dataextractor/container/execution/store"
	"github.com/streamsets/dataextractor/container/util"
	"time"
)

type StandaloneRunner struct {
	pipelineId       string
	config           execution.Config
	validTransitions map[string][]string
	pipelineState    *common.PipelineState
	pipelineConfig   common.PipelineConfiguration
	prodPipeline     *ProductionPipeline
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

	// load offset from file
	var err error

	standaloneRunner.pipelineState, err = store.GetState()
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

func (standaloneRunner *StandaloneRunner) StartPipeline(pipelineId string) (*common.PipelineState, error) {
	var err error
	err = standaloneRunner.checkState(common.STARTING)
	if err != nil {
		return nil, err
	}

	standaloneRunner.pipelineConfig, err = creation.LoadPipelineConfig(pipelineId)
	if err != nil {
		return nil, err
	}

	standaloneRunner.prodPipeline, err = NewProductionPipeline(
		standaloneRunner.config,
		standaloneRunner,
		standaloneRunner.pipelineConfig,
		nil,
	)
	if err != nil {
		return nil, err
	}

	go standaloneRunner.prodPipeline.Run()

	standaloneRunner.pipelineState.Status = common.RUNNING
	standaloneRunner.pipelineState.TimeStamp = time.Now().UTC()
	err = store.SaveState(standaloneRunner.pipelineState)
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
	err = store.SaveState(standaloneRunner.pipelineState)
	if err != nil {
		return nil, err
	}

	return standaloneRunner.pipelineState, nil
}

func (standaloneRunner *StandaloneRunner) ResetOffset() {
	err := store.ResetOffset()
	if err != nil {
		panic(err)
	}
}

func (standaloneRunner *StandaloneRunner) checkState(toState string) error {
	supportedList := standaloneRunner.validTransitions[standaloneRunner.pipelineState.Status]
	if !util.Contains(supportedList, toState) {
		return errors.New("Cannot change state from " + standaloneRunner.pipelineState.Status +
			" to " + toState)
	}
	return nil
}

func NewStandaloneRunner(config execution.Config) (*StandaloneRunner, error) {
	standaloneRunner := StandaloneRunner{config: config}
	standaloneRunner.init()
	return &standaloneRunner, nil
}
