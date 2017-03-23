package manager

type Manager interface {
	// creates a runner for a given pipeline, the runner will have the current state of the pipeline.
	GetRunner()
}
