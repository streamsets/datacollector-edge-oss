package runner

import (
	"fmt"
	"github.com/streamsets/dataextractor/container/validation"
)

type Pipe interface {
	Init()
	Process()
	Destroy()
}

type StagePipe struct {
	Stage StageRuntime
	InputLanes []string
	OutputLanes []string
	EventLanes []string
}

func (s *StagePipe) Init() ([]validation.Issue)  {
	fmt.Println("Stage Pipe Init")
	issues := s.Stage.Init()
	return issues
}

func (s *StagePipe) Process(pipeBatch *FullPipeBatch)  {
	fmt.Println("Processing Stage - " + s.Stage.config.InstanceName)
	batchMaker := pipeBatch.StartStage(*s)
	batchImpl := pipeBatch.GetBatch(*s)
	s.Stage.Execute("previousOffset", 1, batchImpl, batchMaker)
	pipeBatch.CompleteStage(batchMaker)
}

func (s *StagePipe) Destroy()  {

}

func NewStagePipe(stage StageRuntime) (StagePipe) {
	stagePipe := StagePipe{}
	stagePipe.Stage = stage
	stagePipe.InputLanes = stage.config.InputLanes
	stagePipe.OutputLanes = stage.config.OutputLanes
	stagePipe.EventLanes = stage.config.EventLanes
	return stagePipe
}