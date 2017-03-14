package runner

import (
	"log"
	"github.com/streamsets/dataextractor/tail_dataextractor"
	"fmt"
)

type PipelineRunner struct {
	logger *log.Logger
	tailDataExtractor *tail_dataextractor.TailDataExtractor
	sourceOffset *SourceOffset
}

func (pipelineRunner *PipelineRunner) init() {
	// load offset from file
	var err error
	pipelineRunner.sourceOffset, err = GetOffset()

	if (err != nil) {
		panic(err)
	}

	fmt.Println("Current Offset - " + pipelineRunner.sourceOffset.Offset)
}

func (pipelineRunner *PipelineRunner) StartPipeline() {
	pipelineRunner.tailDataExtractor.Start(pipelineRunner.sourceOffset.Offset)
}

func (pipelineRunner *PipelineRunner) StopPipeline() {
	offset, _ := pipelineRunner.tailDataExtractor.Stop()
	pipelineRunner.sourceOffset.Offset = offset
	err := SaveOffset(pipelineRunner.sourceOffset)
	if (err != nil) {
		panic(err)
	}
	fmt.Println("Stopped Pipeine at offset : " + offset)
}

func (pipelineRunner *PipelineRunner) ResetOffset() {

}

func New(logger *log.Logger) (*PipelineRunner, error) {
	tailDataExtractor, _ := tail_dataextractor.New(logger)
	pipelineRunner := PipelineRunner{logger: logger, tailDataExtractor: tailDataExtractor}
	pipelineRunner.init()
	return &pipelineRunner, nil
}
