package common

type StageContext struct {
	StageConfig       StageConfiguration
	RuntimeParameters map[string]interface{}
}
