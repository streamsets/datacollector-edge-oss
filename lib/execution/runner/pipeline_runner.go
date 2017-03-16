package runner

import (
	"errors"
	"fmt"
	"github.com/streamsets/dataextractor/lib/common"
	"github.com/streamsets/dataextractor/lib/execution/store"
	"github.com/streamsets/dataextractor/tail_dataextractor"
	"log"
	"time"
)

type PipelineRunner struct {
	logger            *log.Logger
	validTransitions  map[string][]string
	tailDataExtractor *tail_dataextractor.TailDataExtractor
	sourceOffset      *common.SourceOffset
	pipelineState     *common.PipelineState
}

func (pipelineRunner *PipelineRunner) init() {
	pipelineRunner.validTransitions = make(map[string][]string)
	pipelineRunner.validTransitions[common.EDITED] = []string{common.STARTING, "Teller"}
	pipelineRunner.validTransitions[common.STARTING] = []string{common.START_ERROR, common.RUNNING, common.STOPPING}
	pipelineRunner.validTransitions[common.START_ERROR] = []string{common.STARTING}
	pipelineRunner.validTransitions[common.RUNNING] = []string{common.RUNNING_ERROR, common.FINISHING, common.STOPPING}
	pipelineRunner.validTransitions[common.RUNNING_ERROR] = []string{common.RETRY, common.RUN_ERROR}
	pipelineRunner.validTransitions[common.RETRY] = []string{common.STARTING, common.STOPPING}
	pipelineRunner.validTransitions[common.RUN_ERROR] = []string{common.STARTING}
	pipelineRunner.validTransitions[common.FINISHING] = []string{common.FINISHED}
	pipelineRunner.validTransitions[common.STOPPING] = []string{common.STOPPED}
	pipelineRunner.validTransitions[common.FINISHED] = []string{common.STARTING}
	pipelineRunner.validTransitions[common.STOPPED] = []string{common.STARTING}

	// load offset from file
	var err error
	pipelineRunner.sourceOffset, err = store.GetOffset()
	if err != nil {
		panic(err)
	}

	pipelineRunner.pipelineState, err = store.GetState()
	if err != nil {
		panic(err)
	}
	fmt.Println(pipelineRunner.pipelineState)
}

func (pipelineRunner *PipelineRunner) StartPipeline() (*common.PipelineState, error) {
	var err error
	err = pipelineRunner.checkState(common.STARTING)
	if err != nil {
		return nil, err
	}

	go pipelineRunner.tailDataExtractor.Start(pipelineRunner.sourceOffset.Offset)

	pipelineRunner.pipelineState.Status = common.RUNNING
	pipelineRunner.pipelineState.TimeStamp = time.Now().UTC()
	err = store.SaveState(pipelineRunner.pipelineState)
	if err != nil {
		return nil, err
	}

	return pipelineRunner.pipelineState, nil
}

func (pipelineRunner *PipelineRunner) StopPipeline() (*common.PipelineState, error) {
	var err error
	err = pipelineRunner.checkState(common.STOPPING)
	if err != nil {
		return nil, err
	}

	offset, _ := pipelineRunner.tailDataExtractor.Stop()
	pipelineRunner.sourceOffset.Offset = offset
	err = store.SaveOffset(pipelineRunner.sourceOffset)
	if err != nil {
		panic(err)
	}
	fmt.Println("Stopped Pipeine at offset : " + offset)

	pipelineRunner.pipelineState.Status = common.STOPPED
	pipelineRunner.pipelineState.TimeStamp = time.Now().UTC()
	err = store.SaveState(pipelineRunner.pipelineState)
	if err != nil {
		return nil, err
	}

	return pipelineRunner.pipelineState, nil
}

func (pipelineRunner *PipelineRunner) ResetOffset() {
	err := store.ResetOffset(pipelineRunner.sourceOffset)
	if err != nil {
		panic(err)
	}
}

func (pipelineRunner *PipelineRunner) checkState(toState string) error {
	supportedList := pipelineRunner.validTransitions[pipelineRunner.pipelineState.Status]
	if !common.Contains(supportedList, toState) {
		return errors.New("Cannot change state from " + pipelineRunner.pipelineState.Status +
			" to " + toState)
	}
	return nil
}

func New(logger *log.Logger) (*PipelineRunner, error) {
	tailDataExtractor, _ := tail_dataextractor.New(logger)
	pipelineRunner := PipelineRunner{logger: logger, tailDataExtractor: tailDataExtractor}
	pipelineRunner.init()
	return &pipelineRunner, nil
}
