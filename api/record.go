package api

// Record represents the unit of data Data Collector Edge pipelines process.
//
// GetHeader method returns the metadata header of the record.
//
// Get method returns the root data field of the record.
type Record interface {
	GetHeader() Header
	Get() Field
	Set(field Field) Field
}

//Header represents metadata about the record
type Header interface {
	GetStageCreator() string

	GetSourceId() string

	GetTrackingId() string

	GetPreviousTrackingId() string

	GetStagesPath() string

	GetErrorDataCollectorId() string

	GetErrorPipelineName() string

	GetErrorMessage() string

	GetErrorStage() string

	GetErrorTimestamp() int64

	GetAttributeNames() []string

	GetAttributes() map[string]string

	SetAttribute(name string, value string)
}
