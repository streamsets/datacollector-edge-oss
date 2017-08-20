package common

import (
	"github.com/streamsets/datacollector-edge/api"
)

type RecordImpl struct {
	header *HeaderImpl
	value  *api.Field
}

func (r *RecordImpl) GetHeader() api.Header {
	return r.header
}

func (r *RecordImpl) Get() api.Field {
	return *r.value
}

func (r *RecordImpl) Set(field api.Field) api.Field {
	oldData := r.value
	r.value = &field
	return *oldData
}

type HeaderImpl struct {
	StageCreator         string                 `json:"stageCreator"`
	SourceId             string                 `json:"sourceId"`
	StagesPath           string                 `json:"stagesPath"`
	TrackingId           string                 `json:"trackingId"`
	PreviousTrackingId   string                 `json:"previousTrackingId"`
	ErrorDataCollectorId string                 `json:"errorDataCollectorId"`
	ErrorPipelineName    string                 `json:"errorPipelineName"`
	ErrorStageInstance   string                 `json:"errorStage"`
	ErrorMessage         string                 `json:"errorMessage"`
	ErrorTimestamp       int64                  `json:"errorTimestamp"`
	Attributes           map[string]interface{} `json:"values"`
}

func (h *HeaderImpl) GetStageCreator() string {
	return h.StageCreator
}

func (h *HeaderImpl) GetSourceId() string {
	return h.SourceId
}

func (h *HeaderImpl) GetTrackingId() string {
	return h.TrackingId
}

func (h *HeaderImpl) GetPreviousTrackingId() string {
	return h.PreviousTrackingId
}

func (h *HeaderImpl) GetStagesPath() string {
	return h.StagesPath
}

func (h *HeaderImpl) GetErrorDataCollectorId() string {
	return h.ErrorDataCollectorId
}

func (h *HeaderImpl) GetErrorPipelineName() string {
	return h.ErrorPipelineName
}

func (h *HeaderImpl) GetErrorMessage() string {
	return h.ErrorMessage
}

func (h *HeaderImpl) GetErrorStage() string {
	return h.ErrorStageInstance
}

func (h *HeaderImpl) GetErrorTimestamp() int64 {
	return h.ErrorTimestamp
}

func (h *HeaderImpl) GetAttributeNames() []string {
	var attributeNames []string
	for k := range h.Attributes {
		attributeNames = append(attributeNames, k)
	}
	return attributeNames
}

func (h *HeaderImpl) GetAttributes() map[string]string {
	//Copy map
	attributes := make(map[string]string)
	for k := range h.Attributes {
		attributes[k] = h.Attributes[k].(string)
	}
	return attributes
}

func (h *HeaderImpl) SetAttribute(name string, value string) {
	h.Attributes[name] = value
}

func (h *HeaderImpl) SetStageCreator(stageCreator string) {
	h.StageCreator = stageCreator
}

func (h *HeaderImpl) SetSourceId(sourceId string) {
	h.SourceId = sourceId
}

func (h *HeaderImpl) SetTrackingId(trackingId string) {
	h.TrackingId = trackingId
}

func (h *HeaderImpl) SetErrorTimeStamp(timeStamp int64) {
	h.ErrorTimestamp = timeStamp
}

func (h *HeaderImpl) SetErrorStageInstance(errorStageInstance string) {
	h.ErrorStageInstance = errorStageInstance
}

func (h *HeaderImpl) SetErrorMessage(errorMessage string) {
	h.ErrorMessage = errorMessage
}

func (h *HeaderImpl) SetErrorPipelineName(errorPipelineName string) {
	h.ErrorPipelineName = errorPipelineName
}

func (h *HeaderImpl) SetErrorDataCollectorId(errorDataCollectorId string) {
	h.ErrorDataCollectorId = errorDataCollectorId
}

func createRecord(recordSourceId string, value interface{}) (api.Record, error) {
	var rootField *api.Field
	var err error

	if value != nil {
		rootField, err = api.CreateField(value)
		if err != nil {
			return nil, err
		}
	}

	headerImpl := &HeaderImpl{Attributes: make(map[string]interface{})}
	r := &RecordImpl{
		header: headerImpl,
		value:  rootField,
	}
	headerImpl.SetSourceId(recordSourceId)
	return r, nil
}
