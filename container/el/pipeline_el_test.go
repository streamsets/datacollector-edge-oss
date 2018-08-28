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
	"testing"
	"time"
)

func TestPipelineEL(test *testing.T) {
	pipelineId := "samplePipelineId"
	pipelineTitle := "Sample Pipeline"
	pipelineUser := "admin"
	pipelineStartTime := time.Now()
	evaluationTests := []EvaluationTest{
		{
			Name:       "Test pipeline:id()",
			Expression: "${pipeline:id()}",
			Expected:   pipelineId,
		},
		{
			Name:       "Test function pipeline:id() - Error 1",
			Expression: "${pipeline:id('invalid param')}",
			Expected:   "The function 'pipeline:id' requires 0 arguments but was passed 1",
			ErrorCase:  true,
		},

		{
			Name:       "Test pipeline:title()",
			Expression: "${pipeline:title()}",
			Expected:   pipelineTitle,
		},
		{
			Name:       "Test function pipeline:title() - Error 1",
			Expression: "${pipeline:title('invalid param')}",
			Expected:   "The function 'pipeline:title' requires 0 arguments but was passed 1",
			ErrorCase:  true,
		},

		{
			Name:       "Test pipeline:user()",
			Expression: "${pipeline:user()}",
			Expected:   pipelineUser,
		},
		{
			Name:       "Test function pipeline:user() - Error 1",
			Expression: "${pipeline:user('invalid param')}",
			Expected:   "The function 'pipeline:user' requires 0 arguments but was passed 1",
			ErrorCase:  true,
		},

		{
			Name:       "Test pipeline:startTime()",
			Expression: "${pipeline:startTime()}",
			Expected:   pipelineStartTime,
		},
		{
			Name:       "Test function pipeline:startTime() - Error 1",
			Expression: "${pipeline:startTime('invalid param')}",
			Expected:   "The function 'pipeline:startTime' requires 0 arguments but was passed 1",
			ErrorCase:  true,
		},
	}

	pipelineELContextValues := map[string]interface{}{
		PipelineIdContextVar:        pipelineId,
		PipelineTitleContextVar:     pipelineTitle,
		PipelineUserContextVar:      pipelineUser,
		PipelineStartTimeContextVar: pipelineStartTime,
	}
	pipelineElContext := context.WithValue(context.Background(), PipelineElContextVar, pipelineELContextValues)

	RunEvaluationTests(evaluationTests, []Definitions{&PipelineEL{Context: pipelineElContext}}, test)
}

func TestPipelineELUndefinedValues(test *testing.T) {
	evaluationTests := []EvaluationTest{
		{
			Name:       "Test pipeline:id()",
			Expression: "${pipeline:id()}",
			Expected:   UndefinedValue,
		},
		{
			Name:       "Test pipeline:title()",
			Expression: "${pipeline:title()}",
			Expected:   UndefinedValue,
		},
		{
			Name:       "Test pipeline:user()",
			Expression: "${pipeline:user()}",
			Expected:   UndefinedValue,
		},
	}
	RunEvaluationTests(evaluationTests, []Definitions{&PipelineEL{Context: context.Background()}}, test)
}
