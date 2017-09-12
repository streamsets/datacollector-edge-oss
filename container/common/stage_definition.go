package common

const CONFIG_DEF_TAG_NAME = "ConfigDef"

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
