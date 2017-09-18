package creation

import (
	"errors"
	"fmt"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/el"
	"github.com/streamsets/datacollector-edge/container/util"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"reflect"
	"github.com/streamsets/datacollector-edge/api/configtype"
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

	configMap := stageConfig.GetConfigurationMap()
	reflectValue := reflect.ValueOf(stageInstance).Elem()
	reflectType := reflect.TypeOf(stageInstance).Elem()
	err = injectStageConfigs(reflectValue, reflectType, "", configMap, stageDefinition, runtimeParameters)
	if err != nil {
		return stageBean, err
	}

	stageBean.SystemConfigs = NewStageConfigBean(stageConfig)
	return stageBean, err
}

func injectStageConfigs(
	reflectValue reflect.Value,
	reflectType reflect.Type,
	configPrefix string,
	configMap map[string]common.Config,
	stageDefinition *common.StageDefinition,
	runtimeParameters map[string]interface{},
) error {
	for i := 0; i < reflectValue.NumField(); i++ {
		stageInstanceField := reflectValue.Field(i)
		stageInstanceFieldType := reflectType.Field(i)

		configDefTag := stageInstanceFieldType.Tag.Get(common.CONFIG_DEF_TAG_NAME)
		if len(configDefTag) > 0 {
			configName := configPrefix + util.LcFirst(stageInstanceFieldType.Name)
			configDef := stageDefinition.ConfigDefinitionsMap[configName]
			config := configMap[configName]
			if configDef != nil {
				resolvedValue, err := getResolvedValue(config.Value, runtimeParameters)
				if err != nil {
					return err
				}
				if resolvedValue != nil {
					if stageInstanceField.CanSet() {
						switch configDef.Type {
						case configtype.BOOLEAN:
							stageInstanceField.SetBool(resolvedValue.(bool))
						case configtype.NUMBER:
							stageInstanceField.SetFloat(resolvedValue.(float64))
						case configtype.STRING:
							stageInstanceField.SetString(resolvedValue.(string))
						case configtype.LIST:
							stageInstanceField.Set(reflect.ValueOf(resolvedValue))
						case configtype.MAP:
							listOfMap := resolvedValue.([]interface{})
							mapFieldValue := make(map[string]string)
							for _, mapValue := range listOfMap {
								key := mapValue.(map[string]interface{})["key"].(string)
								value := mapValue.(map[string]interface{})["value"].(string)
								mapFieldValue[key] = value
							}
							stageInstanceField.Set(reflect.ValueOf(mapFieldValue))
						case configtype.MODEL:
							listBeanModelTag := stageInstanceFieldType.Tag.Get(common.LIST_BEAN_MODEL_TAG_NAME)
							if len(listBeanModelTag) > 0 {
								listBeanModelType := stageInstanceFieldType.Type.Elem()

								switch reflect.TypeOf(resolvedValue).Kind() {
								case reflect.Slice:
									listBeanValueList := resolvedValue.([]interface{})
									for _, listBeanValue := range listBeanValueList {
										fmt.Println(listBeanValue)

										listBeanModelField := reflect.New(listBeanModelType)

										err := injectListBeanStageConfigs(
											listBeanModelField.Elem(),
											listBeanModelType,
											"",
											listBeanValue.(map[string]interface{}),
											configDef.Model.ConfigDefinitionsMap,
											runtimeParameters,
										)
										if err != nil {
											return err
										}
										stageInstanceField.Set(reflect.Append(
											stageInstanceField,
											listBeanModelField.Elem(),
										))
									}
								}
							}
						default:
							return errors.New(fmt.Sprintf(
								"Unsupported Field Type %s",
								reflect.TypeOf(stageInstanceField),
							))
						}
					}
				}
			}
		} else {
			configDefBeanTag := stageInstanceFieldType.Tag.Get(common.CONFIG_DEF_BEAN_TAG_NAME)
			if len(configDefBeanTag) > 0 {
				newConfigPrefix := configPrefix + util.LcFirst(stageInstanceFieldType.Name) + "."
				err := injectStageConfigs(
					stageInstanceField,
					stageInstanceFieldType.Type,
					newConfigPrefix,
					configMap,
					stageDefinition,
					runtimeParameters,
				)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func injectListBeanStageConfigs(
	reflectValue reflect.Value,
	reflectType reflect.Type,
	configPrefix string,
	configMap map[string]interface{},
	configDefinitionsMap map[string]*common.ConfigDefinition,
	runtimeParameters map[string]interface{},
) error {
	for i := 0; i < reflectValue.NumField(); i++ {
		stageInstanceField := reflectValue.Field(i)
		stageInstanceFieldType := reflectType.Field(i)

		configDefTag := stageInstanceFieldType.Tag.Get(common.CONFIG_DEF_TAG_NAME)
		if len(configDefTag) > 0 {
			configName := configPrefix + util.LcFirst(stageInstanceFieldType.Name)
			configDef := configDefinitionsMap[configName]
			configValue := configMap[configName]
			if configDef != nil {
				resolvedValue, err := getResolvedValue(configValue, runtimeParameters)
				if err != nil {
					return err
				}
				if resolvedValue != nil {
					if stageInstanceField.CanSet() {
						switch configDef.Type {
						case configtype.BOOLEAN:
							stageInstanceField.SetBool(resolvedValue.(bool))
						case configtype.NUMBER:
							stageInstanceField.SetFloat(resolvedValue.(float64))
						case configtype.STRING:
							stageInstanceField.SetString(resolvedValue.(string))
						case configtype.LIST:
							switch resolvedValue.(type) {
							case []interface{}:
								if len(resolvedValue.([]interface{})) > 0 {
									stageInstanceField.Set(reflect.ValueOf(resolvedValue))
								}
							case []string:
								if len(resolvedValue.([]string)) > 0 {
									stageInstanceField.Set(reflect.ValueOf(resolvedValue))
								}
							}
						case configtype.MAP:
							listOfMap := resolvedValue.([]interface{})
							mapFieldValue := make(map[string]string)
							for _, mapValue := range listOfMap {
								key := mapValue.(map[string]interface{})["key"].(string)
								value := mapValue.(map[string]interface{})["value"].(string)
								mapFieldValue[key] = value
							}
							stageInstanceField.Set(reflect.ValueOf(mapFieldValue))
						default:
							return errors.New(fmt.Sprintf(
								"Unsupported Field Type %s",
								reflect.TypeOf(stageInstanceField),
							))
						}
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
