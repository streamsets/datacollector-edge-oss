package api

type ErrorMessage struct {
	ErrorCode          string `json:"errorCode"`
	Timestamp          int64  `json:"timestamp"`
	LocalizableMessage string `json:"localized"`
	Stacktrace         string `json:"errorStackTrace"`
}
