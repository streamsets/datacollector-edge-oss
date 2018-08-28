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

func TestJobEL(test *testing.T) {
	jobId := "sampleJobId"
	jobName := "Sample Job"
	jobUser := "user1@org1"
	jobStartTime := time.Now()
	evaluationTests := []EvaluationTest{
		{
			Name:       "Test job:id()",
			Expression: "${job:id()}",
			Expected:   jobId,
		},
		{
			Name:       "Test function job:id() - Error 1",
			Expression: "${job:id('invalid param')}",
			Expected:   "The function 'job:id' requires 0 arguments but was passed 1",
			ErrorCase:  true,
		},

		{
			Name:       "Test job:name()",
			Expression: "${job:name()}",
			Expected:   jobName,
		},
		{
			Name:       "Test function job:name() - Error 1",
			Expression: "${job:name('invalid param')}",
			Expected:   "The function 'job:name' requires 0 arguments but was passed 1",
			ErrorCase:  true,
		},

		{
			Name:       "Test job:user()",
			Expression: "${job:user()}",
			Expected:   jobUser,
		},
		{
			Name:       "Test function job:user() - Error 1",
			Expression: "${job:user('invalid param')}",
			Expected:   "The function 'job:user' requires 0 arguments but was passed 1",
			ErrorCase:  true,
		},

		{
			Name:       "Test job:startTime()",
			Expression: "${job:startTime()}",
			Expected:   jobStartTime,
		},
		{
			Name:       "Test function job:startTime() - Error 1",
			Expression: "${job:startTime('invalid param')}",
			Expected:   "The function 'job:startTime' requires 0 arguments but was passed 1",
			ErrorCase:  true,
		},
	}

	jobELContextValues := map[string]interface{}{
		JobIdContextVar:        jobId,
		JobNameContextVar:      jobName,
		JobUserContextVar:      jobUser,
		JobStartTimeContextVar: jobStartTime,
	}
	jobElContext := context.WithValue(context.Background(), JobElContextVar, jobELContextValues)
	RunEvaluationTests(evaluationTests, []Definitions{&JobEL{Context: jobElContext}}, test)
}

func TestJobELUndefinedValues(test *testing.T) {
	evaluationTests := []EvaluationTest{
		{
			Name:       "Test job:id()",
			Expression: "${job:id()}",
			Expected:   UndefinedValue,
		},
		{
			Name:       "Test job:name()",
			Expression: "${job:name()}",
			Expected:   UndefinedValue,
		},
		{
			Name:       "Test job:user()",
			Expression: "${job:user()}",
			Expected:   UndefinedValue,
		},
	}
	RunEvaluationTests(evaluationTests, []Definitions{&JobEL{Context: context.Background()}}, test)
}
