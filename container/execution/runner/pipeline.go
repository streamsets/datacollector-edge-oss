package runner

import (
	"github.com/streamsets/dataextractor/container/common"
	"github.com/streamsets/dataextractor/container/creation"
	"github.com/streamsets/dataextractor/container/validation"
	"log"
)

type Pipeline struct {
	name             string
	standaloneRunner *StandaloneRunner
	pipelineConf     common.PipelineConfiguration
	pipelineBean     creation.PipelineBean
	pipes            []StagePipe
	offsetTracker    SourceOffsetTracker
	stop             bool
}

func (p *Pipeline) Init() []validation.Issue {
	var issues []validation.Issue
	for _, stagePipe := range p.pipes {
		stageIssues := stagePipe.Init()
		issues = append(issues, stageIssues...)
	}

	return issues
}

func (p *Pipeline) Run() {
	log.Println("Pipeline Run()")

	for !p.offsetTracker.IsFinished() && !p.stop {
		p.runBatch()
	}

}

func (p *Pipeline) runBatch() {
	// var committed bool = false
	pipeBatch := NewFullPipeBatch(p.offsetTracker, 1)

	// sourceOffset := pipeBatch.GetPreviousOffset();

	for _, pipe := range p.pipes {
		pipe.Process(pipeBatch)
	}

}

func (p *Pipeline) Stop() {
	log.Println("Pipeline Stop()")
	p.stop = true
}

func NewPipeline(
	standaloneRunner *StandaloneRunner,
	sourceOffsetTracker SourceOffsetTracker,
	runtimeConstants map[string]interface{},
) *Pipeline {

	pipelineBean := creation.NewPipelineBean(standaloneRunner.GetPipelineConfig())

	stageRuntimeList := make([]StageRuntime, len(standaloneRunner.pipelineConfig.Stages))
	pipes := make([]StagePipe, len(standaloneRunner.pipelineConfig.Stages))

	for i, stageBean := range pipelineBean.Stages {
		stageRuntimeList[i] = NewStageRuntime(pipelineBean, stageBean)
		pipes[i] = NewStagePipe(stageRuntimeList[i])
	}

	return &Pipeline{
		standaloneRunner: standaloneRunner,
		pipelineConf:     standaloneRunner.GetPipelineConfig(),
		pipelineBean:     pipelineBean,
		pipes:            pipes,
		offsetTracker:    sourceOffsetTracker,
	}
}
