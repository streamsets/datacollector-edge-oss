package runner

import (
	"github.com/streamsets/dataextractor/container/common"
	"github.com/streamsets/dataextractor/container/execution"
	"log"
)

type ProductionPipeline struct {
	PipelineConfig common.PipelineConfiguration
	Pipeline       *Pipeline
}

func (p *ProductionPipeline) Run() {
	log.Println("[DEBUG] Production Pipeline Run")
	p.Pipeline.Init()
	p.Pipeline.Run()
}

func (p *ProductionPipeline) Stop() {
	log.Println("[DEBUG] Production Pipeline Stop")
	p.Pipeline.Stop()
}

func (p *ProductionPipeline) WasStopped() bool {
	return false
}

func NewProductionPipeline(
	pipelineId string,
	config execution.Config,
	standaloneRunner *StandaloneRunner,
	pipelineConfiguration common.PipelineConfiguration,
	runtimeParameters map[string]interface{},
) (*ProductionPipeline, error) {
	var sourceOffsetTracker SourceOffsetTracker = NewProductionSourceOffsetTracker(pipelineId)
	pipeline, err := NewPipeline(config, standaloneRunner, sourceOffsetTracker, runtimeParameters)
	return &ProductionPipeline{
		PipelineConfig: pipelineConfiguration,
		Pipeline:       pipeline,
	}, err
}
