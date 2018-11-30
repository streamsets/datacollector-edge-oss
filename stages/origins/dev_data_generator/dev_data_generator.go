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
package dev_random

import (
	"github.com/icrowley/fake"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/linkedhashmap"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"math"
	"math/big"
	"math/rand"
	"strconv"
	"time"
)

const (
	Library                     = "streamsets-datacollector-dev-lib"
	StageName                   = "com_streamsets_pipeline_stage_devtest_RandomDataGeneratorSource"
	MapRootType                 = "MAP"
	ListMapRootType             = "LIST_MAP"
	STRING                      = "STRING"
	INTEGER                     = "INTEGER"
	LONG                        = "LONG"
	FLOAT                       = "FLOAT"
	DOUBLE                      = "DOUBLE"
	DATE                        = "DATE"
	DATETIME                    = "DATETIME"
	ZONED_DATETIME              = "ZONED_DATETIME"
	TIME                        = "TIME"
	BOOLEAN                     = "BOOLEAN"
	DECIMAL                     = "DECIMAL"
	BYTE_ARRAY                  = "BYTE_ARRAY"
	LONG_SEQUENCE               = "LONG_SEQUENCE"
	ADDRESS_FULL_ADDRESS        = "ADDRESS_FULL_ADDRESS"
	ADDRESS_BUILDING_NUMBER     = "ADDRESS_BUILDING_NUMBER"
	ADDRESS_STREET_ADDRESS      = "ADDRESS_STREET_ADDRESS"
	ADDRESS_CITY                = "ADDRESS_CITY"
	ADDRESS_STATE               = "ADDRESS_STATE"
	ADDRESS_COUNTRY             = "ADDRESS_COUNTRY"
	ADDRESS_LATITUDE            = "ADDRESS_LATITUDE"
	ADDRESS_LONGITUDE           = "ADDRESS_LONGITUDE"
	APP_NAME                    = "APP_NAME"
	APP_AUTHOR                  = "APP_AUTHOR"
	APP_VERSION                 = "APP_VERSION"
	BUSINESS_CREDIT_CARD_NUMBER = "BUSINESS_CREDIT_CARD_NUMBER"
	BUSINESS_CREDIT_CARD_TYPE   = "BUSINESS_CREDIT_CARD_TYPE"
	COLOR                       = "COLOR"
	COMPANY_NAME                = "COMPANY_NAME"
	COMPANY_INDUSTRY            = "COMPANY_INDUSTRY"
	COMPANY_BUZZWORD            = "COMPANY_BUZZWORD"
	COMPANY_URL                 = "COMPANY_URL"
	DEMOGRAPHIC                 = "DEMOGRAPHIC"
	EMAIL                       = "EMAIL"
	FILE                        = "FILE"
	FINANCE                     = "FINANCE"
	INTERNET                    = "INTERNET"
	LOREM                       = "LOREM"
	MUSIC                       = "MUSIC"
	NAME                        = "NAME"
	PHONENUMBER                 = "PHONENUMBER"
	RACE                        = "RACE"
	SEX                         = "SEX"
	SHAKESPEARE                 = "SHAKESPEARE"
	SPACE                       = "SPACE"
	SSN                         = "SSN"
	STOCK                       = "STOCK"
	NotSupported                = "Not Supported"
)

var randomOffset = "random"
var race = []string{
	"American Indian or Alaska Native",
	"Asian",
	"Black or African American",
	"Native Hawaiian or Other Pacific Islander",
	"White",
}

type Origin struct {
	*common.BaseStage
	DataGenConfigs []DataGeneratorConfig `ConfigDef:"type=MODEL" ListBeanModel:"name=dataGenConfigs"`
	Delay          float64               `ConfigDef:"type=NUMBER,required=true"`
	BatchSize      float64               `ConfigDef:"type=NUMBER,required=true"`
	EventName      string                `ConfigDef:"type=STRING,required=true"`
	RootFieldType  string                `ConfigDef:"type=STRING,required=true"`
}

type DataGeneratorConfig struct {
	Field     string  `ConfigDef:"type=STRING,required=true"`
	Type      string  `ConfigDef:"type=STRING,required=true"`
	Precision float64 `ConfigDef:"type=NUMBER,required=true"`
	Scale     float64 `ConfigDef:"type=NUMBER,required=true"`
}

func init() {
	stagelibrary.SetCreator(Library, StageName, func() api.Stage {
		return &Origin{BaseStage: &common.BaseStage{}}
	})
}

func (d *Origin) Init(stageContext api.StageContext) []validation.Issue {
	issues := d.BaseStage.Init(stageContext)
	return issues
}

func (d *Origin) Produce(lastSourceOffset *string, maxBatchSize int, batchMaker api.BatchMaker) (*string, error) {
	if lastSourceOffset != nil {
		time.Sleep(time.Duration(d.Delay) * time.Millisecond)
	}

	r := rand.New(rand.NewSource(99))
	min := time.Date(2001, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2018, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min

	batchSize := math.Min(float64(maxBatchSize), d.BatchSize)

	for i := 0; i < int(batchSize); i++ {
		var rootField *api.Field
		if d.RootFieldType == MapRootType {
			rootField, _ = d.createMapTypeField(r, delta, min)
		} else {
			rootField, _ = d.createListMapTypeField(r, delta, min)
		}
		recordId := common.CreateRecordId("dev-data", i)
		if record, err := d.GetStageContext().CreateRecord(recordId, map[string]interface{}{}); err == nil {
			record.Set(rootField)
			batchMaker.AddRecord(record)
		} else {
			d.GetStageContext().ToError(err, record)
		}
	}

	var rootEventField *api.Field
	if d.RootFieldType == MapRootType {
		rootEventField, _ = d.createMapTypeField(r, delta, min)
	} else {
		rootEventField, _ = d.createListMapTypeField(r, delta, min)
	}
	recordId := common.CreateRecordId("dev-data-event", 1)
	if eventRecord, err := d.GetStageContext().CreateEventRecord(
		recordId,
		map[string]interface{}{},
		d.EventName,
		1,
	); err == nil {
		eventRecord.Set(rootEventField)
		d.GetStageContext().ToEvent(eventRecord)
	} else {
		d.GetStageContext().ToError(err, eventRecord)
	}

	return &randomOffset, nil
}

func (d *Origin) createMapTypeField(r *rand.Rand, delta int64, min int64) (*api.Field, error) {
	var rootField = make(map[string]*api.Field)
	for _, config := range d.DataGenConfigs {
		rootField[config.Field], _ = d.createField(config, r, delta, min)
	}
	return api.CreateMapFieldWithMapOfFields(rootField), nil
}

func (d *Origin) createListMapTypeField(r *rand.Rand, delta int64, min int64) (*api.Field, error) {
	var listMapField = linkedhashmap.New()
	for _, config := range d.DataGenConfigs {
		field, _ := d.createField(config, r, delta, min)
		listMapField.Put(config.Field, field)
	}
	return api.CreateListMapFieldWithMapOfFields(listMapField), nil
}

func (d *Origin) createField(config DataGeneratorConfig, r *rand.Rand, delta int64, min int64) (*api.Field, error) {
	switch config.Type {
	case BOOLEAN:
		return api.CreateBoolField(r.Int63()&(1<<62) == 0)
	case DATE:
		fallthrough
	case TIME:
		fallthrough
	case DATETIME:
		sec := r.Int63n(delta) + min
		return api.CreateDateTimeField((time.Unix(sec, 0)))
	case DOUBLE:
		return api.CreateDoubleField(r.Float64())
	case FLOAT:
		return api.CreateFloatField(r.Float32())
	case INTEGER:
		return api.CreateIntegerField(r.Int())
	case LONG:
		return api.CreateLongField(r.Int63())
	case STRING:
		return api.CreateStringField(fake.Sentence())
	case DECIMAL:
		i := new(big.Int)
		i.SetString("64443234234123423", 10)
		return api.CreateBigIntField(*i)
	case ADDRESS_FULL_ADDRESS:
		return api.CreateStringField(fake.StreetAddress())
	case ADDRESS_BUILDING_NUMBER:
		return api.CreateStringField(fake.StreetAddress())
	case ADDRESS_STREET_ADDRESS:
		return api.CreateStringField(fake.StreetAddress())
	case ADDRESS_CITY:
		return api.CreateStringField(fake.City())
	case ADDRESS_STATE:
		return api.CreateStringField(fake.State())
	case ADDRESS_COUNTRY:
		return api.CreateStringField(fake.Country())
	case ADDRESS_LATITUDE:
		return api.CreateFloatField(fake.Latitude())
	case ADDRESS_LONGITUDE:
		return api.CreateFloatField(fake.Longitude())
	case APP_NAME:
		return api.CreateStringField(fake.ProductName())
	case APP_AUTHOR:
		return api.CreateStringField(fake.FullName())
	case APP_VERSION:
		return api.CreateIntegerField(fake.MonthNum())
	case BUSINESS_CREDIT_CARD_NUMBER:
		return api.CreateStringField(fake.CreditCardNum(fake.CreditCardType()))
	case BUSINESS_CREDIT_CARD_TYPE:
		return api.CreateStringField(fake.CreditCardType())
	case COLOR:
		return api.CreateStringField(fake.Color())
	case COMPANY_NAME:
		return api.CreateStringField(fake.Company())
	case COMPANY_INDUSTRY:
		return api.CreateStringField(fake.Industry())
	case COMPANY_BUZZWORD:
		return api.CreateStringField(fake.ProductName())
	case COMPANY_URL:
		return api.CreateStringField(fake.DomainName())
	case DEMOGRAPHIC:
		return api.CreateStringField(fake.Characters())
	case EMAIL:
		return api.CreateStringField(fake.EmailAddress())
	case FINANCE:
		return api.CreateStringField(fake.Currency())
	case INTERNET:
		return api.CreateStringField(fake.DomainName())
	case LOREM:
		return api.CreateStringField(fake.Sentence())
	case NAME:
		return api.CreateStringField(fake.FullName())
	case PHONENUMBER:
		return api.CreateStringField(fake.Phone())
	case RACE:
		raceIndex := randIntRange(r, 0, len(race)-1)
		return api.CreateStringField(race[raceIndex])
	case SEX:
		return api.CreateStringField(fake.Gender())
	case SHAKESPEARE:
		return api.CreateStringField(fake.Sentences())
	case SSN:
		return api.CreateStringField(
			strconv.Itoa(randIntRange(r, 100000000, 999999999)),
		)
	case STOCK:
		return api.CreateStringField(fake.CurrencyCode())
	default:
		return api.CreateStringField(NotSupported)
	}
}

func randIntRange(r *rand.Rand, min, max int) int {
	if min == max {
		return min
	}
	return r.Intn((max+1)-min) + min
}
