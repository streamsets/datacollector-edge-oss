package dev_random

import (
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"math/rand"
	"strings"
	"time"
)

const (
	LIBRARY     = "streamsets-datacollector-dev-lib"
	STAGE_NAME  = "com_streamsets_pipeline_stage_devtest_RandomSource"
	CONF_FIELDS = "fields"
	CONF_DELAY  = "delay"
)

type DevRandom struct {
	*common.BaseStage
	fields []string
	delay  float64
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &DevRandom{BaseStage: &common.BaseStage{}}
	})
}

func (d *DevRandom) Init(stageContext api.StageContext) error {
	if err := d.BaseStage.Init(stageContext); err != nil {
		return err
	}
	stageConfig := d.GetStageConfig()
	for _, config := range stageConfig.Configuration {
		resolvedConfigValue, err := stageContext.GetResolvedValue(config.Value)
		if err != nil {
			return err
		}
		if config.Name == CONF_FIELDS {
			d.fields = strings.Split(resolvedConfigValue.(string), ",")
		} else if config.Name == CONF_DELAY {
			d.delay = resolvedConfigValue.(float64)
		}
	}
	return nil
}

func (d *DevRandom) Produce(lastSourceOffset string, maxBatchSize int, batchMaker api.BatchMaker) (string, error) {
	r := rand.New(rand.NewSource(99))
	time.Sleep(time.Duration(d.delay) * time.Millisecond)
	for i := 0; i < maxBatchSize; i++ {
		var recordValue = make(map[string]interface{})
		for _, field := range d.fields {
			recordValue[field] = r.Int()
		}
		record, err := d.GetStageContext().CreateRecord("dev-random", recordValue)
		if err != nil {
			panic(err)
		}
		batchMaker.AddRecord(record)
	}
	return "random", nil
}
