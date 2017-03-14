package task

type Task interface {
	// Returns the task name
	GetName()

	// Initializes the task
	Init()

	// Runs the task
	Run()

	// Stops the task
	Stop()

	// Returns the current status of the task
	GetStatus()
}
