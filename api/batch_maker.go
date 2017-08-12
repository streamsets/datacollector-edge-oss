package api

// BatchMaker is the interface that wraps the basic methods for adding record to pipeline.
// Data Collector Edge origin stages receive an instance of a BatchMaker to write to the pipeline the records they
// create or process.
//
// GetLanes returns the available lane names (stream names) for the stage.
//
// AddRecord adds a record to the BatchMaker.
type BatchMaker interface {
	GetLanes() []string
	AddRecord(record Record)
}
