package common

const (
	PIPELINE_CONFIG_SCHEMA_VERSION = 3
	PIPELINE_CONFIG_VERSION        = 6
)

type PipelineConfiguration struct {
	SchemaVersion        int                    `json:"schemaVersion"`
	Version              int                    `json:"version"`
	PipelineId           string                 `json:"pipelineId"`
	Title                string                 `json:"title"`
	Description          string                 `json:"description"`
	UUID                 string                 `json:"uuid"`
	Configuration        []Config               `json:"configuration"`
	UiInfo               map[string]interface{} `json:"uiInfo"`
	Stages               []StageConfiguration   `json:"stages"`
	ErrorStage           StageConfiguration     `json:"errorStage"`
	StatsAggregatorStage StageConfiguration     `json:"statsAggregatorStage"`
	Previewable          bool                   `json:"previewable"`
	Info                 PipelineInfo           `json:"info"`
	Metadata             map[string]interface{} `json:"metadata"`
}

type PipelineInfo struct {
	PipelineId   string                 `json:"pipelineId"`
	Title        string                 `json:"title"`
	Description  string                 `json:"description"`
	Created      int64                  `json:"created"`
	LastModified int64                  `json:"lastModified"`
	Creator      string                 `json:"creator"`
	LastModifier string                 `json:"lastModifier"`
	LastRev      string                 `json:"lastRev"`
	UUID         string                 `json:"uuid"`
	Valid        bool                   `json:"valid"`
	Metadata     map[string]interface{} `json:"metadata"`
}

type Config struct {
	Name  string      `json:"name"`
	Value interface{} `json:"value"`
}

type StageConfiguration struct {
	InstanceName  string                 `json:"instanceName"`
	Library       string                 `json:"library"`
	StageName     string                 `json:"stageName"`
	StageVersion  string                 `json:"stageVersion"`
	Configuration []Config               `json:"configuration"`
	UiInfo        map[string]interface{} `json:"uiInfo"`
	InputLanes    []string               `json:"inputLanes"`
	OutputLanes   []string               `json:"outputLanes"`
	EventLanes    []string               `json:"eventLanes"`
}

func (s StageConfiguration) GetConfigurationMap() map[string]Config {
	configurationMap := make(map[string]Config)
	for _, config := range s.Configuration {
		configurationMap[config.Name] = config
	}
	return configurationMap
}

type PipelineEnvelope struct {
	PipelineConfig     PipelineConfiguration  `json:"pipelineConfig"`
	PipelineRules      map[string]interface{} `json:"pipelineRules"`
	LibraryDefinitions map[string]interface{} `json:"libraryDefinitions"`
}
