package stagelibrary

import (
	"errors"
	"fmt"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/util"
	"reflect"
	"strings"
	"sync"
)

type NewStageCreator func() api.Stage

var reg *registry

type registry struct {
	sync.RWMutex
	newStageCreatorMap map[string]NewStageCreator
	stageDefinitionMap map[string]*common.StageDefinition
}

func init() {
	reg = new(registry)
	reg.newStageCreatorMap = make(map[string]NewStageCreator)
	reg.stageDefinitionMap = make(map[string]*common.StageDefinition)
}

func SetCreator(library string, stageName string, newStageCreator NewStageCreator) {
	stageKey := library + ":" + stageName
	reg.Lock()
	reg.newStageCreatorMap[stageKey] = newStageCreator
	reg.Unlock()
}

func GetCreator(library string, stageName string) (NewStageCreator, bool) {
	stageKey := library + ":" + stageName
	reg.RLock()
	s, b := reg.newStageCreatorMap[stageKey]
	reg.RUnlock()
	return s, b
}

func CreateStageInstance(library string, stageName string) (api.Stage, *common.StageDefinition, error) {
	if t, ok := GetCreator(library, stageName); ok {
		v := t()

		stageDefinition := extractStageDefinition(library, stageName, v)
		return v, stageDefinition, nil
	} else {
		return nil, nil, errors.New("No Stage Instance found for : " + library + ", stage: " + stageName)
	}
}

func extractStageDefinition(library string, stageName string, stageInstance interface{}) *common.StageDefinition {
	stageDefinition := &common.StageDefinition{
		Name:                 stageName,
		Library:              library,
		ConfigDefinitionsMap: make(map[string]*common.ConfigDefinition),
	}
	t := reflect.TypeOf(stageInstance).Elem()
	extractConfigDefinitions(t, "", stageDefinition)
	return stageDefinition
}

func extractConfigDefinitions(
	t reflect.Type,
	configPrefix string,
	stageDefinition *common.StageDefinition,
) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		configDefTag := field.Tag.Get(common.CONFIG_DEF_TAG_NAME)
		if len(configDefTag) > 0 {
			extractConfigDefinition(field, configDefTag, configPrefix, stageDefinition)
		} else {
			configDefBeanTag := field.Tag.Get(common.CONFIG_DEF_BEAN_TAG_NAME)
			if len(configDefBeanTag) > 0 {
				newConfigPrefix := configPrefix + util.LcFirst(field.Name) + "."
				extractConfigDefinitions(field.Type, newConfigPrefix, stageDefinition)
			}
		}
	}
}

func extractConfigDefinition(
	field reflect.StructField,
	configDefTag string,
	configPrefix string,
	stageDefinition *common.StageDefinition,
) {
	configDef := &common.ConfigDefinition{}
	configDefTagValues := strings.Split(configDefTag, ",")
	for _, tagValue := range configDefTagValues {
		args := strings.Split(tagValue, "=")
		switch args[0] {
		case "type":
			fmt.Sscanf(tagValue, "type=%s", &configDef.Type)
		case "required":
			fmt.Sscanf(tagValue, "required=%t", &configDef.Required)
		}
	}
	configDef.Name = configPrefix + util.LcFirst(field.Name)
	stageDefinition.ConfigDefinitionsMap[configDef.Name] = configDef
}
