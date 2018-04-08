/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package common

import (
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
)

type BaseStage struct {
	stageContext api.StageContext
}

func (b *BaseStage) GetStageContext() api.StageContext {
	return b.stageContext
}

func (b *BaseStage) Init(stageContext api.StageContext) []validation.Issue {
	issues := make([]validation.Issue, 0)
	b.stageContext = stageContext
	return issues
}

func (b *BaseStage) Destroy() error {
	//No OP Destroy
	return nil
}

func (b *BaseStage) GetStageConfig() *StageConfiguration {
	return b.stageContext.(*StageContextImpl).StageConfig
}
