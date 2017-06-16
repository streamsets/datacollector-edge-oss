package runner

import (
	"github.com/rcrowley/go-metrics"
	"github.com/streamsets/sdc2go/container/common"
	"github.com/streamsets/sdc2go/container/execution"
	"log"
)

type ProductionPipeline struct {
	PipelineConfig common.PipelineConfiguration
	Pipeline       *Pipeline
	MetricRegistry metrics.Registry
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
	sourceOffsetTracker := NewProductionSourceOffsetTracker(pipelineId)
	metricRegistry := metrics.NewRegistry()
	pipeline, err := NewPipeline(config, standaloneRunner, sourceOffsetTracker, runtimeParameters, metricRegistry)
	return &ProductionPipeline{
		PipelineConfig: pipelineConfiguration,
		Pipeline:       pipeline,
		MetricRegistry: metricRegistry,
	}, err
}
