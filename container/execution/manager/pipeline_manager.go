package manager

import (
	"github.com/streamsets/dataextractor/container/common"
	"github.com/streamsets/dataextractor/container/execution"
	"github.com/streamsets/dataextractor/container/execution/runner"
)

type PipelineManager struct {
	config    execution.Config
	runnerMap map[string]*runner.StandaloneRunner
}

func (p *PipelineManager) GetRunner(pipelineId string) *runner.StandaloneRunner {
	if p.runnerMap[pipelineId] == nil {
		pRunner, err := runner.NewStandaloneRunner(pipelineId, p.config)
		if err != nil {
			panic(err)
		}
		p.runnerMap[pipelineId] = pRunner
	}
	return p.runnerMap[pipelineId]
}

func (p *PipelineManager) StartPipeline(pipelineId string) (*common.PipelineState, error) {
	return p.GetRunner(pipelineId).StartPipeline()
}

func (p *PipelineManager) StopPipeline(pipelineId string) (*common.PipelineState, error) {
	return p.GetRunner(pipelineId).StopPipeline()
}

func (p *PipelineManager) ResetOffset(pipelineId string) error {
	return p.GetRunner(pipelineId).ResetOffset()
}

func New(config execution.Config) (*PipelineManager, error) {
	pipelineManager := PipelineManager{config: config, runnerMap: make(map[string]*runner.StandaloneRunner)}

	return &pipelineManager, nil
}
