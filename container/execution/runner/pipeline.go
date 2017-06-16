package runner

import (
	"github.com/rcrowley/go-metrics"
	"github.com/streamsets/sdc2go/container/common"
	"github.com/streamsets/sdc2go/container/creation"
	"github.com/streamsets/sdc2go/container/execution"
	"github.com/streamsets/sdc2go/container/util"
	"github.com/streamsets/sdc2go/container/validation"
	"log"
	"time"
)

type Pipeline struct {
	name             string
	config           execution.Config
	standaloneRunner *StandaloneRunner
	pipelineConf     common.PipelineConfiguration
	pipelineBean     creation.PipelineBean
	pipes            []Pipe
	offsetTracker    SourceOffsetTracker
	stop             bool
	errorSink        *common.ErrorSink

	MetricRegistry              metrics.Registry
	batchProcessingTimer        metrics.Timer
	batchCountCounter           metrics.Counter
	batchInputRecordsCounter    metrics.Counter
	batchOutputRecordsCounter   metrics.Counter
	batchErrorRecordsCounter    metrics.Counter
	batchErrorMessagesCounter   metrics.Counter
	batchCountMeter             metrics.Meter
	batchInputRecordsMeter      metrics.Meter
	batchOutputRecordsMeter     metrics.Meter
	batchErrorRecordsMeter      metrics.Meter
	batchErrorMessagesMeter     metrics.Meter
	batchInputRecordsHistogram  metrics.Histogram
	batchOutputRecordsHistogram metrics.Histogram
	batchErrorRecordsHistogram  metrics.Histogram
	batchErrorMessagesHistogram metrics.Histogram
}

const (
	AT_MOST_ONCE                      = "AT_MOST_ONCE"
	AT_LEAST_ONCE                     = "AT_LEAST_ONCE"
	PIPELINE_BATCH_PROCESSING         = "pipeline.batchProcessing"
	PIPELINE_BATCH_COUNT              = "pipeline.batchCount"
	PIPELINE_BATCH_INPUT_RECORDS      = "pipeline.batchInputRecords"
	PIPELINE_BATCH_OUTPUT_RECORDS     = "pipeline.batchOutputRecords"
	PIPELINE_BATCH_ERROR_RECORDS      = "pipeline.batchErrorRecords"
	PIPELINE_BATCH_ERROR_MESSAGES     = "pipeline.batchErrorMessages"
	PIPELINE_INPUT_RECORDS_PER_BATCH  = "pipeline.inputRecordsPerBatch"
	PIPELINE_OUTPUT_RECORDS_PER_BATCH = "pipeline.outputRecordsPerBatch"
	PIPELINE_ERROR_RECORDS_PER_BATCH  = "pipeline.errorRecordsPerBatch"
	PIPELINE_ERRORS_PER_BATCH         = "pipeline.errorsPerBatch"
)

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
	start := time.Now()

	p.errorSink.ClearErrorRecordsAndMesssages()

	pipeBatch := NewFullPipeBatch(p.offsetTracker, 1, p.errorSink)

	for _, pipe := range p.pipes {
		if p.pipelineBean.Config.DeliveryGuarantee == AT_MOST_ONCE &&
			pipe.IsTarget() && // if destination
			!committed {
			p.offsetTracker.CommitOffset()
			committed = true
		}

		err := pipe.Process(pipeBatch)
		if err != nil {
			log.Println("[ERROR] ", err)
		}
	}

	if p.pipelineBean.Config.DeliveryGuarantee == AT_LEAST_ONCE {
		p.offsetTracker.CommitOffset()
	}

	p.batchProcessingTimer.UpdateSince(start)
	p.batchCountCounter.Inc(1)
	p.batchCountMeter.Mark(1)

	p.batchInputRecordsCounter.Inc(pipeBatch.GetInputRecords())
	p.batchOutputRecordsCounter.Inc(pipeBatch.GetOutputRecords())
	p.batchErrorMessagesCounter.Inc(pipeBatch.GetErrorMessages())
	p.batchErrorRecordsCounter.Inc(pipeBatch.GetErrorRecords())

	p.batchInputRecordsMeter.Mark(pipeBatch.GetInputRecords())
	p.batchOutputRecordsMeter.Mark(pipeBatch.GetOutputRecords())
	p.batchErrorMessagesMeter.Mark(pipeBatch.GetErrorMessages())
	p.batchErrorRecordsMeter.Mark(pipeBatch.GetErrorRecords())

	p.batchInputRecordsHistogram.Update(pipeBatch.GetInputRecords())
	p.batchOutputRecordsHistogram.Update(pipeBatch.GetOutputRecords())
	p.batchErrorMessagesHistogram.Update(pipeBatch.GetErrorMessages())
	p.batchErrorRecordsHistogram.Update(pipeBatch.GetErrorRecords())

}

func (p *Pipeline) Stop() {
	log.Println("[DEBUG] Pipeline Stop()")
	for _, stagePipe := range p.pipes {
		stagePipe.Destroy()
	}
	p.stop = true
}

func NewPipeline(
	config execution.Config,
	standaloneRunner *StandaloneRunner,
	sourceOffsetTracker SourceOffsetTracker,
	runtimeParameters map[string]interface{},
	metricRegistry metrics.Registry,
) (*Pipeline, error) {
	pipelineBean, err := creation.NewPipelineBean(standaloneRunner.GetPipelineConfig())
	if err != nil {
		return nil, err
	}

	stageRuntimeList := make([]StageRuntime, len(standaloneRunner.pipelineConfig.Stages))
	pipes := make([]Pipe, len(standaloneRunner.pipelineConfig.Stages))
	errorSink := common.NewErrorSink()

	var resolvedParameters = make(map[string]interface{})
	for k, v := range pipelineBean.Config.Constants {
		if runtimeParameters != nil && runtimeParameters[k] != nil {
			resolvedParameters[k] = runtimeParameters[k]
		} else {
			resolvedParameters[k] = v
		}
	}

	for i, stageBean := range pipelineBean.Stages {
		stageContext := &common.StageContextImpl{
			StageConfig: stageBean.Config,
			Parameters:  resolvedParameters,
			Metrics:     metricRegistry,
			ErrorSink:   errorSink,
		}
		stageRuntimeList[i] = NewStageRuntime(pipelineBean, stageBean, stageContext)
		pipes[i] = NewStagePipe(stageRuntimeList[i], config)
	}

	p := &Pipeline{
		standaloneRunner: standaloneRunner,
		pipelineConf:     standaloneRunner.GetPipelineConfig(),
		pipelineBean:     pipelineBean,
		pipes:            pipes,
		errorSink:        errorSink,
		offsetTracker:    sourceOffsetTracker,
		MetricRegistry:   metricRegistry,
	}

	p.batchProcessingTimer = util.CreateTimer(metricRegistry, PIPELINE_BATCH_PROCESSING)

	p.batchCountCounter = util.CreateCounter(metricRegistry, PIPELINE_BATCH_COUNT)
	p.batchInputRecordsCounter = util.CreateCounter(metricRegistry, PIPELINE_BATCH_INPUT_RECORDS)
	p.batchOutputRecordsCounter = util.CreateCounter(metricRegistry, PIPELINE_BATCH_OUTPUT_RECORDS)
	p.batchErrorRecordsCounter = util.CreateCounter(metricRegistry, PIPELINE_BATCH_ERROR_RECORDS)
	p.batchErrorMessagesCounter = util.CreateCounter(metricRegistry, PIPELINE_BATCH_ERROR_MESSAGES)

	p.batchCountMeter = util.CreateMeter(metricRegistry, PIPELINE_BATCH_COUNT)
	p.batchInputRecordsMeter = util.CreateMeter(metricRegistry, PIPELINE_BATCH_INPUT_RECORDS)
	p.batchOutputRecordsMeter = util.CreateMeter(metricRegistry, PIPELINE_BATCH_OUTPUT_RECORDS)
	p.batchErrorRecordsMeter = util.CreateMeter(metricRegistry, PIPELINE_BATCH_ERROR_RECORDS)
	p.batchErrorMessagesMeter = util.CreateMeter(metricRegistry, PIPELINE_BATCH_ERROR_MESSAGES)

	p.batchInputRecordsHistogram = util.CreateHistogram5Min(metricRegistry, PIPELINE_INPUT_RECORDS_PER_BATCH)
	p.batchOutputRecordsHistogram = util.CreateHistogram5Min(metricRegistry, PIPELINE_OUTPUT_RECORDS_PER_BATCH)
	p.batchErrorRecordsHistogram = util.CreateHistogram5Min(metricRegistry, PIPELINE_ERROR_RECORDS_PER_BATCH)
	p.batchErrorMessagesHistogram = util.CreateHistogram5Min(metricRegistry, PIPELINE_ERRORS_PER_BATCH)

	return p, nil
}
