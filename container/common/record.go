package common

import (
	"github.com/streamsets/sdc2go/api"
)

type RecordImpl struct {
	header *HeaderImpl
	value  interface{}
}

func (r *RecordImpl) GetHeader() api.Header {
	return r.header
}

func (r *RecordImpl) GetValue() interface{} {
	return r.value
}

type HeaderImpl struct {
	stageCreator         string
	sourceId             string
	stagesPath           string
	trackingId           string
	previousTrackingId   string
	errorDataCollectorId string
	errorPipelineName    string
	errorStageInstance   string
	errorMessage         string
	errorTimestamp       int64
	attributes           map[string]interface{}
}

func (h *HeaderImpl) GetStageCreator() string {
	return h.stageCreator
}

func (h *HeaderImpl) GetSourceId() string {
	return h.sourceId
}

func (h *HeaderImpl) GetTrackingId() string {
	return h.trackingId
}

func (h *HeaderImpl) GetPreviousTrackingId() string {
	return h.previousTrackingId
}

func (h *HeaderImpl) GetStagesPath() string {
	return h.stagesPath
}

func (h *HeaderImpl) GetErrorDataCollectorId() string {
	return h.errorDataCollectorId
}

func (h *HeaderImpl) GetErrorPipelineName() string {
	return h.errorPipelineName
}

func (h *HeaderImpl) GetErrorMessage() string {
	return h.errorMessage
}

func (h *HeaderImpl) GetErrorStage() string {
	return h.errorStageInstance
}

func (h *HeaderImpl) GetErrorTimestamp() int64 {
	return h.errorTimestamp
}

func (h *HeaderImpl) GetAttributeNames() []string {
	var attributeNames []string
	for k := range h.attributes {
		attributeNames = append(attributeNames, k)
	}
	return attributeNames
}

func (h *HeaderImpl) GetAttributes() map[string]string {
	//Copy map
	attributes := make(map[string]string)
	for k := range h.attributes {
		attributes[k] = h.attributes[k].(string)
	}
	return attributes
}

func (h *HeaderImpl) SetAttribute(name string, value string) {
	h.attributes[name] = value
}

func (h *HeaderImpl) SetStageCreator(stageCreator string) {
	h.stageCreator = stageCreator
}

func (h *HeaderImpl) SetSourceId(sourceId string) {
	h.sourceId = sourceId
}

func (h *HeaderImpl) SetTrackingId(trackingId string) {
	h.trackingId = trackingId
}

func (h *HeaderImpl) SetErrorTimeStamp(timeStamp int64) {
	h.errorTimestamp = timeStamp
}

func (h *HeaderImpl) SetErrorStageInstance(errorStageInstance string) {
	h.errorStageInstance = errorStageInstance
}

func (h *HeaderImpl) SetErrorMessage(errorMessage string) {
	h.errorMessage = errorMessage
}

func (h *HeaderImpl) SetErrorPipelineName(errorPipelineName string) {
	h.errorPipelineName = errorPipelineName
}

func (h *HeaderImpl) SetErrorDataCollectorId(errorDataCollectorId string) {
	h.errorDataCollectorId = errorDataCollectorId
}

func createRecord(recordSourceId string, value interface{}) api.Record {
	headerImpl := &HeaderImpl{attributes: make(map[string]interface{})}
	r := &RecordImpl{header: headerImpl, value: value}
	headerImpl.SetSourceId(recordSourceId)
	return r
}
