package runner

import (
	"fmt"
	"github.com/streamsets/dataextractor/container/common"
)

type ProductionPipeline struct {
	PipelineConfig common.PipelineConfiguration
	Pipeline       *Pipeline
}

func (p *ProductionPipeline) Run() {
	fmt.Println("Production Pipeline Run")
	p.Pipeline.Init()
	p.Pipeline.Run()

}

func (p *ProductionPipeline) Stop() {
	fmt.Println("Production Pipeline Stop")
	p.Pipeline.Stop()
}

func (p *ProductionPipeline) WasStopped() bool {
	return false
}

func NewProductionPipeline(
	standaloneRunner *StandaloneRunner,
	pipelineConfiguration common.PipelineConfiguration,
	runtimeConstants map[string]interface{},
) *ProductionPipeline {
	var sourceOffsetTracker SourceOffsetTracker
	sourceOffsetTracker = NewProductionSourceOffsetTracker("pipelineId")

	pipeline := NewPipeline(standaloneRunner, sourceOffsetTracker, runtimeConstants)

	return &ProductionPipeline{
		PipelineConfig: pipelineConfiguration,
		Pipeline:       pipeline,
	}
}
