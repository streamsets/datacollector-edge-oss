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
package selector

import (
	"context"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/el"
	"github.com/streamsets/datacollector-edge/container/util"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
)

const (
	LIBRARY           = "streamsets-datacollector-basic-lib"
	STAGE_NAME        = "com_streamsets_pipeline_stage_processor_selector_SelectorDProcessor"
	VERSION           = 1
	OUTPUT_LANE       = "outputLane"
	PREDICATE         = "predicate"
	SELECTOR_02_ERROR = "The Stream Selector does not define the output stream '%s' associated with condition '%s'"
	SELECTOR_07_ERROR = "The last condition must be 'default'"
	DEFAULT           = "default"
)

type SelectorProcessor struct {
	*common.BaseStage
	LanePredicates []map[string]string `ConfigDef:"type=MODEL,evaluation=EXPLICIT" PredicateModel:"name=lanePredicates"`
	defaultLane    string
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &SelectorProcessor{BaseStage: &common.BaseStage{}}
	})
}

func (s *SelectorProcessor) Init(stageContext api.StageContext) []validation.Issue {
	issues := s.BaseStage.Init(stageContext)

	err := s.parsePredicateLanes()
	if err != nil {
		issues = append(issues, stageContext.CreateConfigIssue(err.Error()))
		return issues
	}

	if s.LanePredicates[len(s.LanePredicates)-1][PREDICATE] != DEFAULT {
		issues = append(issues, stageContext.CreateConfigIssue(SELECTOR_07_ERROR))
		return issues
	} else {
		s.defaultLane = s.LanePredicates[len(s.LanePredicates)-1][OUTPUT_LANE]
	}

	return issues
}

func (s *SelectorProcessor) parsePredicateLanes() error {
	for _, predicateLaneMap := range s.LanePredicates {
		if !util.Contains(s.GetStageContext().GetOutputLanes(), predicateLaneMap[OUTPUT_LANE]) {
			return errors.New(fmt.Sprintf(SELECTOR_02_ERROR, predicateLaneMap[OUTPUT_LANE], predicateLaneMap[PREDICATE]))
		}
	}
	return nil
}

func (s *SelectorProcessor) Process(batch api.Batch, batchMaker api.BatchMaker) error {
	for _, record := range batch.GetRecords() {
		recordContext := context.WithValue(context.Background(), el.RecordContextVar, record)
		matchedAtLeastOnePredicate := false
		for _, predicateLaneMap := range s.LanePredicates {
			if predicateLaneMap[OUTPUT_LANE] != s.defaultLane {
				evaluateRes, err := s.GetStageContext().Evaluate(predicateLaneMap[PREDICATE], PREDICATE, recordContext)

				if err != nil {
					log.WithError(err).Error("Error evaluating record")
					s.GetStageContext().ToError(err, record)
				}

				if evaluateRes.(bool) {
					matchedAtLeastOnePredicate = true
					batchMaker.AddRecord(record, predicateLaneMap[OUTPUT_LANE])
				}
			}
		}

		if !matchedAtLeastOnePredicate {
			batchMaker.AddRecord(record, s.defaultLane)
		}
	}
	return nil
}
