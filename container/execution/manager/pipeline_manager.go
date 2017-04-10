package manager

import (
	"github.com/streamsets/dataextractor/container/execution"
	"github.com/streamsets/dataextractor/container/execution/runner"
)

type PipelineManager struct {
	runner *runner.StandaloneRunner
}

func (pipelineManager *PipelineManager) GetRunner() *runner.StandaloneRunner {
	return pipelineManager.runner
}

func New(config execution.Config) (*PipelineManager, error) {
	pipelineRunner, _ := runner.NewStandaloneRunner(config)
	pipelineManager := PipelineManager{runner: pipelineRunner}

	return &pipelineManager, nil
}
