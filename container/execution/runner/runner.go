package runner

import "github.com/streamsets/sdc2go/container/common"

type Runner interface {
	GetPipelineConfig() common.PipelineConfiguration
	GetStatus() (*common.PipelineState, error)
	StartPipeline(pipelineId string) (*common.PipelineState, error)
	StopPipeline(*common.PipelineState, error)
	ResetOffset()
}
