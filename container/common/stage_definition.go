package common

const (
	CONFIG_DEF_TAG_NAME      = "ConfigDef"
	CONFIG_DEF_BEAN_TAG_NAME = "ConfigDefBean"
	LIST_BEAN_MODEL_TAG_NAME = "ListBeanModel"
	PREDICATE_MODEL_TAG_NAME = "PredicateModel"
)

type StageDefinition struct {
	Name                 string
	Library              string
	Version              string
	ConfigDefinitionsMap map[string]*ConfigDefinition
}

type ConfigDefinition struct {
	Name      string
	Type      string
	Required  bool
	FieldName string
	Model     ModelDefinition
}

type ModelDefinition struct {
	ConfigDefinitionsMap map[string]*ConfigDefinition
}
