package runner

import (
	"github.com/streamsets/dataextractor/container/common"
	"log"
)

type ProductionPipeline struct {
	PipelineConfig common.PipelineConfiguration
	Pipeline       *Pipeline
}

func (p *ProductionPipeline) Run() {
	log.Println("Production Pipeline Run")
	p.Pipeline.Init()
	p.Pipeline.Run()

}

func (p *ProductionPipeline) Stop() {
	log.Println("Production Pipeline Stop")
	p.Pipeline.Stop()
}

func (p *ProductionPipeline) WasStopped() bool {
	return false
}

func NewProductionPipeline(
	standaloneRunner *StandaloneRunner,
	pipelineConfiguration common.PipelineConfiguration,
	runtimeConstants map[string]interface{},
) (*ProductionPipeline, error) {
	var sourceOffsetTracker SourceOffsetTracker
	sourceOffsetTracker = NewProductionSourceOffsetTracker("pipelineId")
	pipeline, err := NewPipeline(standaloneRunner, sourceOffsetTracker, runtimeConstants)
	return &ProductionPipeline{
		PipelineConfig: pipelineConfiguration,
		Pipeline:       pipeline,
	}, err
}
