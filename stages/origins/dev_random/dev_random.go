package dev_random

import (
	"context"
	"github.com/streamsets/dataextractor/api"
	"github.com/streamsets/dataextractor/container/common"
	"github.com/streamsets/dataextractor/stages/stagelibrary"
	"math/rand"
	"strings"
	"time"
)

const (
	LIBRARY    = "streamsets-datacollector-dev-lib"
	STAGE_NAME = "com_streamsets_pipeline_stage_devtest_RandomSource"
)

type DevRandom struct {
	fields []string
	delay  float64
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &DevRandom{}
	})
}

func (d DevRandom) Init(ctx context.Context) {
	stageContext := (ctx.Value("stageContext")).(common.StageContext)
	stageConfig := stageContext.StageConfig
	for _, config := range stageConfig.Configuration {
		if config.Name == "fields" {
			d.fields = strings.SplitAfter(config.Value.(string), ",")
		} else if config.Name == "delay" {
			d.delay = config.Value.(float64)
		}
	}
}

func (d DevRandom) Destroy() {
}

func (d DevRandom) Produce(lastSourceOffset string, maxBatchSize int, batchMaker api.BatchMaker) (string, error) {
	r := rand.New(rand.NewSource(99))

	time.Sleep(time.Duration(d.delay) * time.Millisecond)

	for i := 0; i < maxBatchSize; i++ {
		var recordValue = make(map[string]int)
		for _, field := range d.fields {
			recordValue[field] = r.Int()
		}
		batchMaker.AddRecord(api.Record{Value: recordValue})
	}

	return "random", nil
}