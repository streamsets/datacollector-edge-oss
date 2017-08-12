package api

// Destination is a Data Collector Edge destination stage. Destination stages receive records from origin
// stages and write them to an external system.
//
// Write method, when running a pipeline, the Data Collector Edge calls this method from the Destination stage to write
// a batch of records to an external system.
type Destination interface {
	Write(batch Batch) error
}
