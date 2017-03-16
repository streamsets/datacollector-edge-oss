package manager

import (
	"github.com/streamsets/dataextractor/lib/execution/runner"
	"log"
)

type PipelineManager struct {
	logger *log.Logger
	runner *runner.PipelineRunner
}

func (pipelineManager *PipelineManager) GetRunner() *runner.PipelineRunner {
	return pipelineManager.runner
}

func New(logger *log.Logger) (*PipelineManager, error) {
	pipelineRunner, _ := runner.New(logger)
	pipelineManager := PipelineManager{logger: logger, runner: pipelineRunner}

	return &pipelineManager, nil
}
