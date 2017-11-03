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
package runner

import (
	"github.com/streamsets/datacollector-edge/api"
)

type BatchMakerImpl struct {
	stagePipe        StagePipe
	stageOutput      map[string][]api.Record
	singleOutputLane string
	size             int64
}

func (b *BatchMakerImpl) GetLanes() []string {
	return b.stagePipe.OutputLanes
}

func (b *BatchMakerImpl) AddRecord(record api.Record, outputLanes ...string) {
	if len(outputLanes) > 0 {
		//This is a bit costly we are cloning all records (greedy) before going to the stages
		//We can do better by simply cloning for the immediate stage which is going to process
		//To save up on memory
		for _, outputLane := range outputLanes {
			b.stageOutput[outputLane] = append(b.stageOutput[outputLane], record.Clone())
		}
	} else {
		b.stageOutput[b.singleOutputLane] = append(b.stageOutput[b.singleOutputLane], record.Clone())
	}
	b.size++
}

func (b *BatchMakerImpl) GetStageOutput(outputLane ...string) []api.Record {
	if len(outputLane) > 0 {
		return b.stageOutput[outputLane[0]]
	}
	return b.stageOutput[b.singleOutputLane]
}

func (b *BatchMakerImpl) GetSize() int64 {
	return b.size
}

func NewBatchMakerImpl(stagePipe StagePipe) *BatchMakerImpl {
	batchMaker := &BatchMakerImpl{stagePipe: stagePipe}
	batchMaker.stageOutput = make(map[string][]api.Record)
	for _, outputLane := range stagePipe.OutputLanes {
		batchMaker.stageOutput[outputLane] = make([]api.Record, 0)
	}
	if len(stagePipe.OutputLanes) > 0 {
		batchMaker.singleOutputLane = stagePipe.OutputLanes[0]
	}
	return batchMaker
}
