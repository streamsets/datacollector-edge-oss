package runner

import (
	"github.com/streamsets/dataextractor/container/validation"
	"log"
)

const BATCH_SIZE = 10

type Pipe interface {
	Init()
	Process()
	Destroy()
}

type StagePipe struct {
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
	s.Stage.Execute("previousOffset", BATCH_SIZE, batchImpl, batchMaker)
	pipeBatch.CompleteStage(batchMaker)
}

func (s *StagePipe) Destroy() {

}

func NewStagePipe(stage StageRuntime) StagePipe {
	stagePipe := StagePipe{}
	stagePipe.Stage = stage
	stagePipe.InputLanes = stage.config.InputLanes
	stagePipe.OutputLanes = stage.config.OutputLanes
	stagePipe.EventLanes = stage.config.EventLanes
	return stagePipe
}
