package api

// Batch is the interface that wraps the basic Batch method.
//
// GetSourceOffset returns the initial offset of the current batch.
// This return value should be treated as an opaque value as it is source dependent.
//
// GetRecords returns an iterator with all the records in the batch for the current stage.
// Every time this method is called it returns a new iterator with all records in the batch.
type Batch interface {
	GetSourceOffset() string
	GetRecords() []Record
}
