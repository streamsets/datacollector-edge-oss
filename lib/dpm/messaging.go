package dpm

type ClientEvent struct {
	EventId      string   `json:"eventId"`
	Destinations []string `json:"destinations"`
	RequiresAck  bool     `json:"requiresAck"`
	IsAckEvent   bool     `json:"isAckEvent"`
	EventTypeId  int      `json:"eventTypeId"`
	Payload      string   `json:"payload"`
	OrgId        string   `json:"orgId"`
}

type ServerEvent struct {
	EventId      string `json:"eventId"`
	From         string `json:"from"`
	RequiresAck  bool   `json:"requiresAck"`
	IsAckEvent   bool   `json:"isAckEvent"`
	EventTypeId  int    `json:"eventTypeId"`
	Payload      string `json:"payload"`
	ReceivedTime int64  `json:"receivedTime"`
	OrgId        string `json:"orgId"`
}


