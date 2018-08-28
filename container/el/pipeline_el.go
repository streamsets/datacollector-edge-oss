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
	PipelineElContextVar        = "PIPELINE_EL"
	PipelineIdContextVar        = "PIPELINE_ID"
	PipelineTitleContextVar     = "PIPELINE_TITLE"
	PipelineUserContextVar      = "PIPELINE_USER"
	PipelineStartTimeContextVar = "PIPELINE_START_TIME"
	UndefinedValue              = "UNDEFINED"
)

type PipelineEL struct {
	Context context.Context
}

func (p *PipelineEL) GetId(args ...interface{}) (interface{}, error) {
	if len(args) != 0 {
		return "", errors.New(
			fmt.Sprintf("The function 'pipeline:id' requires 0 arguments but was passed %d", len(args)),
		)
	}

	if p.Context != nil && p.Context.Value(PipelineElContextVar) != nil {
		pipelineELContextValues := p.Context.Value(PipelineElContextVar).(map[string]interface{})
		return pipelineELContextValues[PipelineIdContextVar], nil
	}

	return UndefinedValue, nil
}

func (p *PipelineEL) GetTitle(args ...interface{}) (interface{}, error) {
	if len(args) != 0 {
		return "", errors.New(
			fmt.Sprintf("The function 'pipeline:title' requires 0 arguments but was passed %d", len(args)),
		)
	}

	if p.Context != nil && p.Context.Value(PipelineElContextVar) != nil {
		pipelineELContextValues := p.Context.Value(PipelineElContextVar).(map[string]interface{})
		return pipelineELContextValues[PipelineTitleContextVar], nil
	}

	return UndefinedValue, nil
}

func (p *PipelineEL) GetUser(args ...interface{}) (interface{}, error) {
	if len(args) != 0 {
		return "", errors.New(
			fmt.Sprintf("The function 'pipeline:user' requires 0 arguments but was passed %d", len(args)),
		)
	}

	if p.Context != nil && p.Context.Value(PipelineElContextVar) != nil {
		pipelineELContextValues := p.Context.Value(PipelineElContextVar).(map[string]interface{})
		return pipelineELContextValues[PipelineUserContextVar], nil
	}

	return UndefinedValue, nil
}

func (p *PipelineEL) GetStartTime(args ...interface{}) (interface{}, error) {
	if len(args) != 0 {
		return "", errors.New(
			fmt.Sprintf("The function 'pipeline:startTime' requires 0 arguments but was passed %d", len(args)),
		)
	}

	if p.Context != nil && p.Context.Value(PipelineElContextVar) != nil {
		pipelineELContextValues := p.Context.Value(PipelineElContextVar).(map[string]interface{})
		return pipelineELContextValues[PipelineStartTimeContextVar], nil
	}

	return time.Now(), nil
}

func (p *PipelineEL) GetELFunctionDefinitions() map[string]govaluate.ExpressionFunction {
	functions := map[string]govaluate.ExpressionFunction{
		"pipeline:id":        p.GetId,
		"pipeline:title":     p.GetTitle,
		"pipeline:user":      p.GetUser,
		"pipeline:startTime": p.GetStartTime,
	}
	return functions
}
