package common

const (
	CONFIG_DEF_TAG_NAME      = "ConfigDef"
	CONFIG_DEF_BEAN_TAG_NAME = "ConfigDefBean"
)

type StageDefinition struct {
	Name                 string
	Library              string
	Version              string
	ConfigDefinitionsMap map[string]*ConfigDefinition
}

type ConfigDefinition struct {
	Name     string
	Type     string
	Required bool
}
