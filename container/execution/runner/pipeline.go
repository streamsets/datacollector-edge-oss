package runner

import (
	"context"
	"github.com/streamsets/dataextractor/container/common"
	"github.com/streamsets/dataextractor/container/creation"
	"github.com/streamsets/dataextractor/container/execution"
	"github.com/streamsets/dataextractor/container/validation"
	"log"
)

type Pipeline struct {
	name             string
	config           execution.Config
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
	log.Println("[DEBUG] Pipeline Run()")

	for !p.offsetTracker.IsFinished() && !p.stop {
		p.runBatch()
	}

}

func (p *Pipeline) runBatch() {
	var committed bool = false
	pipeBatch := NewFullPipeBatch(p.offsetTracker, 1)
	for _, pipe := range p.pipes {
		if p.pipelineBean.Config.DeliveryGuarantee == "AT_MOST_ONCE" &&
			len(pipe.OutputLanes) == 0 && // if destination
			!committed {
			p.offsetTracker.CommitOffset()
			committed = true
		}

		err := pipe.Process(pipeBatch)
		if err != nil {
			log.Println("[ERROR] ", err)
		}
	}

	if p.pipelineBean.Config.DeliveryGuarantee == "AT_LEAST_ONCE" {
		p.offsetTracker.CommitOffset()
	}
}

func (p *Pipeline) Stop() {
	log.Println("[DEBUG] Pipeline Stop()")
	p.stop = true
}

func NewPipeline(
	config execution.Config,
	standaloneRunner *StandaloneRunner,
	sourceOffsetTracker SourceOffsetTracker,
	runtimeParameters map[string]interface{},
) (*Pipeline, error) {

	pipelineBean, err := creation.NewPipelineBean(standaloneRunner.GetPipelineConfig())

	if err != nil {
		return nil, err
	}

	stageRuntimeList := make([]StageRuntime, len(standaloneRunner.pipelineConfig.Stages))
	pipes := make([]StagePipe, len(standaloneRunner.pipelineConfig.Stages))
	pipelineContext := context.Background()

	for i, stageBean := range pipelineBean.Stages {
		stageContext := common.StageContext{
			StageConfig:       stageBean.Config,
			RuntimeParameters: runtimeParameters,
		}
		contextWithValue := context.WithValue(pipelineContext, "stageContext", stageContext)
		stageRuntimeList[i] = NewStageRuntime(pipelineBean, stageBean, contextWithValue)
		pipes[i] = NewStagePipe(stageRuntimeList[i], config)
	}

	return &Pipeline{
		standaloneRunner: standaloneRunner,
		pipelineConf:     standaloneRunner.GetPipelineConfig(),
		pipelineBean:     pipelineBean,
		pipes:            pipes,
		offsetTracker:    sourceOffsetTracker,
	}, nil
}
