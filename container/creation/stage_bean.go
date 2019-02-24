// Copyright 2018 StreamSets Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package creation

import (
	"context"
	"errors"
	"fmt"
	"github.com/spf13/cast"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/configtype"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/el"
	"github.com/streamsets/datacollector-edge/container/util"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"reflect"
	"strconv"
)

const (
	STAGE_TYPE = "stageType"
	SOURCE     = "SOURCE"
	PROCESSOR  = "PROCESSOR"
	TARGET     = "TARGET"
)

type StageBean struct {
	Config        *common.StageConfiguration
	Stage         api.Stage
	SystemConfigs StageConfigBean
	Services      []ServiceBean
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
	stageConfig *common.StageConfiguration,
	runtimeParameters map[string]interface{},
	elContext context.Context,
) (StageBean, error) {
	stageBean := StageBean{}

	stageInstance, stageDefinition, err := stagelibrary.CreateStageInstance(stageConfig.Library, stageConfig.StageName)
	if err != nil {
		return stageBean, err
	}

	stageBean.Config = stageConfig
	stageBean.Stage = stageInstance

	configMap := stageConfig.GetConfigurationMap()
	reflectValue := reflect.ValueOf(stageInstance).Elem()
	reflectType := reflect.TypeOf(stageInstance).Elem()
	err = injectStageConfigs(
		reflectValue,
		reflectType,
		"",
		configMap,
		stageDefinition.ConfigDefinitionsMap,
		runtimeParameters,
		elContext,
	)
	if err != nil {
		return stageBean, err
	}

	stageBean.SystemConfigs = NewStageConfigBean(stageConfig)

	if stageConfig.Services != nil && len(stageConfig.Services) > 0 {
		stageBean.Services = make([]ServiceBean, 0)

		for _, serviceConfig := range stageConfig.Services {
			serviceBean := ServiceBean{}

			serviceInstance, serviceDefinition, err := stagelibrary.CreateServiceInstance(serviceConfig.Service)
			if err != nil {
				return stageBean, err
			}

			serviceBean.Config = serviceConfig
			serviceBean.Service = serviceInstance

			configMap := serviceConfig.GetConfigurationMap()
			reflectValue := reflect.ValueOf(serviceInstance).Elem()
			reflectType := reflect.TypeOf(serviceInstance).Elem()
			err = injectStageConfigs(
				reflectValue,
				reflectType,
				"",
				configMap,
				serviceDefinition.ConfigDefinitionsMap,
				runtimeParameters,
				elContext,
			)
			if err != nil {
				return stageBean, err
			}

			stageBean.Services = append(stageBean.Services, serviceBean)
		}
	}

	return stageBean, err
}

func injectStageConfigs(
	reflectValue reflect.Value,
	reflectType reflect.Type,
	configPrefix string,
	configMap map[string]common.Config,
	configDefinitionsMap map[string]*common.ConfigDefinition,
	runtimeParameters map[string]interface{},
	elContext context.Context,
) error {
	for i := 0; i < reflectValue.NumField(); i++ {
		stageInstanceField := reflectValue.Field(i)
		stageInstanceFieldType := reflectType.Field(i)

		configDefTag := stageInstanceFieldType.Tag.Get(common.ConfigDefTagName)
		if len(configDefTag) > 0 {
			configName := configPrefix + util.LcFirst(stageInstanceFieldType.Name)
			configDef := configDefinitionsMap[configName]
			config := configMap[configName]
			if configDef != nil {
				resolvedValue, err := getResolvedValue(configDef, config.Value, runtimeParameters, elContext)
				if err != nil {
					return err
				}
				if resolvedValue != nil {
					if stageInstanceField.CanSet() {
						switch configDef.Type {
						case configtype.BOOLEAN:
							if reflect.TypeOf(resolvedValue).Kind() == reflect.String {
								var err error
								resolvedValue, err = strconv.ParseBool(cast.ToString(resolvedValue))
								if err != nil {
									return errors.New(fmt.Sprintf("Error when processing value '%v' as BOOLEAN", resolvedValue))
								}
							}
							stageInstanceField.SetBool(cast.ToBool(resolvedValue))
						case configtype.NUMBER:
							if reflect.TypeOf(resolvedValue).Kind() == reflect.String {
								var err error
								resolvedValue, err = strconv.ParseFloat(resolvedValue.(string), 64)
								if err != nil {
									return errors.New(fmt.Sprintf(
										"Error when processing value '%v' as NUMBER for config '%s' : %s",
										resolvedValue,
										configDef.Name,
										err.Error(),
									))
								}
							}
							stageInstanceField.SetFloat(cast.ToFloat64(resolvedValue))
						case configtype.STRING:
							stageInstanceField.SetString(cast.ToString(resolvedValue))
						case configtype.LIST:
							switch resolvedValue.(type) {
							case []interface{}:
								if len(resolvedValue.([]interface{})) > 0 {
									if stageInstanceField.Type() == reflect.TypeOf([]string{}) {
										newValue := make([]string, len(resolvedValue.([]interface{})))
										for i, val := range resolvedValue.([]interface{}) {
											newValue[i] = cast.ToString(val)
										}
										stageInstanceField.Set(reflect.ValueOf(newValue))
									} else {
										stageInstanceField.Set(reflect.ValueOf(resolvedValue))
									}
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
								key := cast.ToString(mapValue.(map[string]interface{})["key"])
								value := cast.ToString(mapValue.(map[string]interface{})["value"])
								mapFieldValue[key] = value
							}
							stageInstanceField.Set(reflect.ValueOf(mapFieldValue))
						case configtype.MODEL:
							listBeanModelTag := stageInstanceFieldType.Tag.Get(common.ListBeanModelTagName)
							if len(listBeanModelTag) > 0 {
								listBeanModelType := stageInstanceFieldType.Type.Elem()

								switch reflect.TypeOf(resolvedValue).Kind() {
								case reflect.Slice:
									listBeanValueList := resolvedValue.([]interface{})
									for _, listBeanValue := range listBeanValueList {
										listBeanModelField := reflect.New(listBeanModelType)

										err := injectListBeanStageConfigs(
											listBeanModelField.Elem(),
											listBeanModelType,
											"",
											listBeanValue.(map[string]interface{}),
											configDef.Model.ConfigDefinitionsMap,
											runtimeParameters,
											elContext,
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
							} else {
								predicateModelTag := stageInstanceFieldType.Tag.Get(common.PredicateModelTagName)
								if len(predicateModelTag) > 0 {
									predicateValueListOfMap := make([]map[string]string, 0)
									switch reflect.TypeOf(resolvedValue).Kind() {
									case reflect.Slice:
										predicateValueList := resolvedValue.([]interface{})
										for _, predicateValue := range predicateValueList {
											predicateValueMap := predicateValue.(map[string]interface{})
											valueMap := map[string]string{
												"outputLane": cast.ToString(predicateValueMap["outputLane"]),
												"predicate":  cast.ToString(predicateValueMap["predicate"]),
											}
											predicateValueListOfMap = append(predicateValueListOfMap, valueMap)
										}
									}
									stageInstanceField.Set(reflect.ValueOf(predicateValueListOfMap))
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
			configDefBeanTag := stageInstanceFieldType.Tag.Get(common.ConfigDefBeanTagName)
			if len(configDefBeanTag) > 0 {
				newConfigPrefix := configPrefix + util.LcFirst(stageInstanceFieldType.Name) + "."
				err := injectStageConfigs(
					stageInstanceField,
					stageInstanceFieldType.Type,
					newConfigPrefix,
					configMap,
					configDefinitionsMap,
					runtimeParameters,
					elContext,
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
	elContext context.Context,
) error {
	for i := 0; i < reflectValue.NumField(); i++ {
		stageInstanceField := reflectValue.Field(i)
		stageInstanceFieldType := reflectType.Field(i)

		configDefTag := stageInstanceFieldType.Tag.Get(common.ConfigDefTagName)
		if len(configDefTag) > 0 {
			configName := configPrefix + util.LcFirst(stageInstanceFieldType.Name)
			configDef := configDefinitionsMap[configName]
			configValue := configMap[configName]
			if configDef != nil {
				resolvedValue, err := getResolvedValue(configDef, configValue, runtimeParameters, elContext)
				if err != nil {
					return err
				}
				if resolvedValue != nil {
					if stageInstanceField.CanSet() {
						switch configDef.Type {
						case configtype.BOOLEAN:
							stageInstanceField.SetBool(cast.ToBool(resolvedValue))
						case configtype.NUMBER:
							stageInstanceField.SetFloat(cast.ToFloat64(resolvedValue))
						case configtype.STRING:
							stageInstanceField.SetString(cast.ToString(resolvedValue))
						case configtype.LIST:
							switch resolvedValue.(type) {
							case []interface{}:
								if len(resolvedValue.([]interface{})) > 0 {
									if stageInstanceField.Type() == reflect.TypeOf([]string{}) {
										newValue := make([]string, len(resolvedValue.([]interface{})))
										for i, val := range resolvedValue.([]interface{}) {
											newValue[i] = cast.ToString(val)
										}
										stageInstanceField.Set(reflect.ValueOf(newValue))
									} else if stageInstanceField.Type() == reflect.TypeOf([]float64{}) {
										newValue := make([]float64, len(resolvedValue.([]interface{})))
										for i, val := range resolvedValue.([]interface{}) {
											newValue[i], err = strconv.ParseFloat(val.(string), 64)
											if err != nil {
												return errors.New(fmt.Sprintf(
													"Error when processing value '%v' as NUMBER",
													resolvedValue,
												))
											}
										}
										stageInstanceField.Set(reflect.ValueOf(newValue))
									} else {
										stageInstanceField.Set(reflect.ValueOf(resolvedValue))
									}
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
								key := cast.ToString(mapValue.(map[string]interface{})["key"])
								value := cast.ToString(mapValue.(map[string]interface{})["value"])
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

func getResolvedValue(
	configDef *common.ConfigDefinition,
	configValue interface{},
	runtimeParameters map[string]interface{},
	elContext context.Context,
) (interface{}, error) {
	var err error
	if configDef.Evaluation == common.EvaluationExplicit {
		return configValue, nil
	}
	switch t := configValue.(type) {
	case string:
		return resolveIfImplicitEL(cast.ToString(configValue), runtimeParameters, elContext)
	case []interface{}:
		for i, val := range t {
			t[i], err = getResolvedValue(configDef, val, runtimeParameters, elContext)
			if err != nil {
				return nil, err
			}
		}
		return configValue, nil
	case map[string]interface{}:
		for k, v := range t {
			t[k], err = getResolvedValue(configDef, v, runtimeParameters, elContext)
			if err != nil {
				return nil, err
			}
		}
		return configValue, nil
	default:
		return configValue, nil
	}
}

func resolveIfImplicitEL(
	configValue string,
	runtimeParameters map[string]interface{},
	elContext context.Context,
) (interface{}, error) {
	if el.IsElString(configValue) {
		return el.Evaluate(configValue, "configName", runtimeParameters, elContext)
	} else {
		return configValue, nil
	}
}
