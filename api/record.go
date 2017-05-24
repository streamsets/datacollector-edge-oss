package api

// Record represents the unit of data Data Extractor pipelines process.
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

	GetRaw() []byte

	GetRawMimeType() string

	GetErrorDataCollectorId() string

	GetErrorPipelineName() string

	GetErrorCode() string

	GetErrorMessage() string

	GetErrorStage() string

	GetErrorTimestamp() int64

	GetErrorStackTrace() string

	GetAttributeNames() []string

	GetAttributes() map[string]string

	SetAttribute(name string, value string)
}
