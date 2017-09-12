package creation

import (
	"errors"
	"fmt"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/el"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"reflect"
)

const (
	STAGE_TYPE = "stageType"
	SOURCE     = "SOURCE"
	PROCESSOR  = "PROCESSOR"
	TARGET     = "TARGET"
)

type StageBean struct {
	Config        common.StageConfiguration
	Stage         api.Stage
	SystemConfigs StageConfigBean
}

func (s *StageBean) IsSource() bool {
	uiInfo := s.Config.UiInfo
	return uiInfo[STAGE_TYPE] == SOURCE
}

func (s *StageBean) IsProcessor() bool {
	uiInfo := s.Config.UiInfo
	return uiInfo[STAGE_TYPE] == PROCESSOR
}

func (s *StageBean) IsTarget() bool {
	uiInfo := s.Config.UiInfo
	return uiInfo[STAGE_TYPE] == TARGET
}

func NewStageBean(
	stageConfig common.StageConfiguration,
	runtimeParameters map[string]interface{},
) (StageBean, error) {
	stageInstance, stageDefinition, err := stagelibrary.CreateStageInstance(stageConfig.Library, stageConfig.StageName)
	stageBean := StageBean{}
	stageBean.Config = stageConfig
	stageBean.Stage = stageInstance

	err = injectStageConfigs(stageInstance, stageConfig, stageDefinition, runtimeParameters)
	if err != nil {
		return stageBean, err
	}

	stageBean.SystemConfigs = NewStageConfigBean(stageConfig)
	return stageBean, err
}

func injectStageConfigs(
	stageInstance api.Stage,
	stageConfig common.StageConfiguration,
	stageDefinition *common.StageDefinition,
	runtimeParameters map[string]interface{},
) error {
	stageInstanceVal := reflect.ValueOf(stageInstance).Elem()

	for _, config := range stageConfig.Configuration {
		configDef := stageDefinition.ConfigDefinitionsMap[config.Name]
		if configDef != nil {
			resolvedValue, err := getResolvedValue(config.Value, runtimeParameters)
			if err != nil {
				return err
			}
			if resolvedValue != nil {
				configField := stageInstanceVal.FieldByName(configDef.Name)
				if configField.CanSet() {
					switch configDef.Type {
					case "BOOLEAN":
						configField.SetBool(resolvedValue.(bool))
					case "NUMBER":
						configField.SetFloat(resolvedValue.(float64))
					case "STRING":
						configField.SetString(resolvedValue.(string))
					default:
						err = errors.New(fmt.Sprintf("Unsupported Field Type %s", reflect.TypeOf(configField)))
					}
				}
			}
		}
	}

	return nil
}

func getResolvedValue(configValue interface{}, runtimeParameters map[string]interface{}) (interface{}, error) {
	var err error
	switch t := configValue.(type) {
	case string:
		return resolveIfImplicitEL(configValue.(string), runtimeParameters)
	case []interface{}:
		for i, val := range t {
			t[i], err = getResolvedValue(val, runtimeParameters)
			if err != nil {
				return nil, err
			}
		}
		return configValue, nil
	case map[string]interface{}:
		for k, v := range t {
			t[k], err = getResolvedValue(v, runtimeParameters)
			if err != nil {
				return nil, err
			}
		}
		return configValue, nil
	default:
		return configValue, nil
	}
	return configValue, nil
}

func resolveIfImplicitEL(configValue string, runtimeParameters map[string]interface{}) (interface{}, error) {
	if el.IsElString(configValue) {
		return el.Evaluate(configValue, "configName", runtimeParameters)
	} else {
		return configValue, nil
	}
}
