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
package common

const (
	ConfigDefTagName      = "ConfigDef"
	ConfigDefBeanTagName  = "ConfigDefBean"
	ListBeanModelTagName  = "ListBeanModel"
	PredicateModelTagName = "PredicateModel"
	EvaluationExplicit    = "EXPLICIT"
	EvaluationImplicit    = "IMPLICIT"
)

type StageDefinition struct {
	Name                 string
	Library              string
	Version              string
	ConfigDefinitionsMap map[string]*ConfigDefinition
}

type ConfigDefinition struct {
	Name       string
	Type       string
	Required   bool
	FieldName  string
	Evaluation string
	Model      ModelDefinition
}

type ModelDefinition struct {
	ConfigDefinitionsMap map[string]*ConfigDefinition
}
