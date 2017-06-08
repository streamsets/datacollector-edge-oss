package api

// Record represents the unit of data sdc2go pipelines process.
type Record interface {
	GetHeader() Header
	GetValue() interface{}
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
