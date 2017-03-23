package manager

import (
	"github.com/streamsets/dataextractor/container/execution/runner"
	"log"
)

type PipelineManager struct {
	logger *log.Logger
	runner *runner.StandaloneRunner
}

func (pipelineManager *PipelineManager) GetRunner() *runner.StandaloneRunner {
	return pipelineManager.runner
}

func New(logger *log.Logger) (*PipelineManager, error) {
	pipelineRunner, _ := runner.NewStandaloneRunner(logger)
	pipelineManager := PipelineManager{logger: logger, runner: pipelineRunner}

	return &pipelineManager, nil
}
