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
package el

import (
	"context"
	"strings"
)

const (
	NAMESPACE_FN_SEPARATOR = ":"
)

func IsElString(configValue string) bool {
	return strings.HasPrefix(configValue, PARAMETER_PREFIX) &&
		strings.HasSuffix(configValue, PARAMETER_SUFFIX)
}

func Evaluate(
	value string,
	configName string,
	parameters map[string]interface{},
	elContext context.Context,
) (interface{}, error) {
	evaluator, _ := NewEvaluator(
		configName,
		parameters,
		[]Definitions{
			&StringEL{},
			&MathEL{},
			&MapListEL{},
			&PipelineEL{Context: elContext},
		},
	)
	return evaluator.Evaluate(value)
}
