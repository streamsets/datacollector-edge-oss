package api

// Record represents the unit of data Data Collector Edge pipelines process.
//
// GetHeader method returns the metadata header of the record.
//
// Get method returns the root data field of the record.
type Record interface {
	GetHeader() Header
	Get(fieldPath ...string) (*Field, error)
	Set(field *Field) *Field
	SetField(fieldPath string, field *Field) (*Field, error)
	GetFieldPaths() map[string]bool
	Delete(fieldPath string) (*Field, error)
	Clone() Record
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
