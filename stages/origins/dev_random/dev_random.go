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
	Fields     string  `ConfigDef:"name=fields,type=STRING,required=true"`
	Delay      float64 `ConfigDef:"name=delay,type=NUMBER,required=true"`
	fieldsList []string
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
	d.fieldsList = strings.Split(d.Fields, ",")
	return nil
}

func (d *DevRandom) Produce(lastSourceOffset string, maxBatchSize int, batchMaker api.BatchMaker) (string, error) {
	r := rand.New(rand.NewSource(99))
	time.Sleep(time.Duration(d.Delay) * time.Millisecond)
	for i := 0; i < maxBatchSize; i++ {
		var recordValue = make(map[string]interface{})
		for _, field := range d.fieldsList {
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
