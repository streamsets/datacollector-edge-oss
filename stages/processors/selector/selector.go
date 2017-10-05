package selector

import (
	"context"
	"errors"
	"fmt"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/el"
	"github.com/streamsets/datacollector-edge/container/util"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"log"
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

func (s *SelectorProcessor) Init(stageContext api.StageContext) error {
	err := s.BaseStage.Init(stageContext)
	if err != nil {
		return err
	}

	err = s.parsePredicateLanes()
	if err != nil {
		return err
	}

	if s.LanePredicates[len(s.LanePredicates)-1][PREDICATE] != DEFAULT {
		return errors.New(SELECTOR_07_ERROR)
	} else {
		s.defaultLane = s.LanePredicates[len(s.LanePredicates)-1][OUTPUT_LANE]
	}

	return err
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
		recordContext := context.WithValue(context.Background(), el.RECORD_CONTEXT_VAR, record)
		matchedAtLeastOnePredicate := false
		for _, predicateLaneMap := range s.LanePredicates {
			if predicateLaneMap[OUTPUT_LANE] != s.defaultLane {
				evaluateRes, err := s.GetStageContext().Evaluate(predicateLaneMap[PREDICATE], PREDICATE, recordContext)

				if err != nil {
					log.Println("[Error] Error evaluating Record", err)
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
