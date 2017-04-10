package runner

import (
	"github.com/streamsets/dataextractor/container/execution"
	"github.com/streamsets/dataextractor/container/validation"
	"log"
)

type Pipe interface {
	Init()
	Process()
	Destroy()
}

type StagePipe struct {
	config      execution.Config
	Stage       StageRuntime
	InputLanes  []string
	OutputLanes []string
	EventLanes  []string
}

func (s *StagePipe) Init() []validation.Issue {
	log.Println("Stage Pipe Init")
	issues := s.Stage.Init()
	return issues
}

func (s *StagePipe) Process(pipeBatch *FullPipeBatch) {
	log.Println("Processing Stage - " + s.Stage.config.InstanceName)
	batchMaker := pipeBatch.StartStage(*s)
	batchImpl := pipeBatch.GetBatch(*s)
	newOffset, _ := s.Stage.Execute(pipeBatch.GetPreviousOffset(), s.config.MaxBatchSize, batchImpl, batchMaker)
	if s.isSource() {
		pipeBatch.SetNewOffset(newOffset)
	}
	pipeBatch.CompleteStage(batchMaker)
}

func (s *StagePipe) Destroy() {

}

func (s *StagePipe) isSource() bool {
	return len(s.OutputLanes) > 0
}

func NewStagePipe(stage StageRuntime, config execution.Config) StagePipe {
	stagePipe := StagePipe{}
	stagePipe.config = config
	stagePipe.Stage = stage
	stagePipe.InputLanes = stage.config.InputLanes
	stagePipe.OutputLanes = stage.config.OutputLanes
	stagePipe.EventLanes = stage.config.EventLanes
	return stagePipe
}
