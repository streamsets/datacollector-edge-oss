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
	issues := s.Stage.Init()
	return issues
}

func (s *StagePipe) Process(pipeBatch *FullPipeBatch) error {
	log.Println("[DEBUG] Processing Stage - " + s.Stage.config.InstanceName)
	batchMaker := pipeBatch.StartStage(*s)
	batchImpl := pipeBatch.GetBatch(*s)
	newOffset, err := s.Stage.Execute(pipeBatch.GetPreviousOffset(), s.config.MaxBatchSize, batchImpl, batchMaker)

	if err != nil {
		return err
	}

	if s.isSource() {
		pipeBatch.SetNewOffset(newOffset)
	}
	pipeBatch.CompleteStage(batchMaker)

	return nil
}

func (s *StagePipe) Destroy() {
	s.Stage.Destroy()
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
