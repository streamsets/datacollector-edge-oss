package manager

import (
	"github.com/streamsets/dataextractor/container/common"
	"github.com/streamsets/dataextractor/container/execution/runner"
)

type Manager interface {
	GetRunner(pipelineId string) *runner.StandaloneRunner
	StartPipeline(pipelineId string) (*common.PipelineState, error)
	StopPipeline(pipelineId string) (*common.PipelineState, error)
	ResetOffset(pipelineId string) (*common.PipelineState, error)
}
