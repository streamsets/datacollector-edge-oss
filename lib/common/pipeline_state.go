package common

import (
	"time"
)

const (
	EDITED        = "EDITED"        // pipeline job has been create/modified, didn't run since the creation/modification
	STARTING      = "STARTING"      // pipeline job starting (initialization)
	START_ERROR   = "START_ERROR"   // pipeline job failed while start (during initialization)
	RUNNING       = "RUNNING"       // pipeline job running
	RUNNING_ERROR = "RUNNING_ERROR" // pipeline job failed while running (calling destroy on pipeline)
	RUN_ERROR     = "RUN_ERROR"     // pipeline job failed while running (done)
	FINISHING     = "FINISHING"     // pipeline job finishing (source reached end, returning NULL offset) (calling destroy on pipeline)
	FINISHED      = "FINISHED"      // pipeline job finished
	RETRY         = "RETRY"         // pipeline job retrying
	STOPPING      = "STOPPING"      // pipeline job has been manually stopped (calling destroy on pipeline)
	STOPPED       = "STOPPED"       // pipeline job has been manually stopped (done)
)

type PipelineState struct {
	Name      string
	Status    string
	Message   string
	TimeStamp time.Time
}