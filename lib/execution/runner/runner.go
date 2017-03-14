package runner

type Runner interface {
	StartPipeline()
	StopPipeline()
	ResetOffset()
}
