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
	"errors"
	"fmt"
	"github.com/madhukard/govaluate"
	"time"
)

const (
	JobElContextVar        = "JOB_EL"
	JobIdContextVar        = "JOB_ID"
	JobNameContextVar      = "JOB_NAME"
	JobUserContextVar      = "JOB_USER"
	JobStartTimeContextVar = "JOB_START_TIME"
)

type JobEL struct {
	Context context.Context
}

func (j *JobEL) GetId(args ...interface{}) (interface{}, error) {
	if len(args) != 0 {
		return "", errors.New(
			fmt.Sprintf("The function 'job:id' requires 0 arguments but was passed %d", len(args)),
		)
	}

	if j.Context != nil && j.Context.Value(JobElContextVar) != nil {
		jobELContextValues := j.Context.Value(JobElContextVar).(map[string]interface{})
		return jobELContextValues[JobIdContextVar], nil
	}

	return UndefinedValue, nil
}

func (j *JobEL) GetName(args ...interface{}) (interface{}, error) {
	if len(args) != 0 {
		return "", errors.New(
			fmt.Sprintf("The function 'job:name' requires 0 arguments but was passed %d", len(args)),
		)
	}

	if j.Context != nil && j.Context.Value(JobElContextVar) != nil {
		jobELContextValues := j.Context.Value(JobElContextVar).(map[string]interface{})
		return jobELContextValues[JobNameContextVar], nil
	}

	return UndefinedValue, nil
}

func (j *JobEL) GetUser(args ...interface{}) (interface{}, error) {
	if len(args) != 0 {
		return "", errors.New(
			fmt.Sprintf("The function 'job:user' requires 0 arguments but was passed %d", len(args)),
		)
	}

	if j.Context != nil && j.Context.Value(JobElContextVar) != nil {
		jobELContextValues := j.Context.Value(JobElContextVar).(map[string]interface{})
		return jobELContextValues[JobUserContextVar], nil
	}

	return UndefinedValue, nil
}

func (j *JobEL) GetStartTime(args ...interface{}) (interface{}, error) {
	if len(args) != 0 {
		return "", errors.New(
			fmt.Sprintf("The function 'job:startTime' requires 0 arguments but was passed %d", len(args)),
		)
	}

	if j.Context != nil && j.Context.Value(JobElContextVar) != nil {
		jobELContextValues := j.Context.Value(JobElContextVar).(map[string]interface{})
		return jobELContextValues[JobStartTimeContextVar], nil
	}

	return time.Now(), nil
}

func (j *JobEL) GetELFunctionDefinitions() map[string]govaluate.ExpressionFunction {
	functions := map[string]govaluate.ExpressionFunction{
		"job:id":        j.GetId,
		"job:name":      j.GetName,
		"job:user":      j.GetUser,
		"job:startTime": j.GetStartTime,
	}
	return functions
}
