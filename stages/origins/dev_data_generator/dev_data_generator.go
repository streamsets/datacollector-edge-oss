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
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"math/big"
	"math/rand"
	"strconv"
	"time"
)

const (
	Library                     = "streamsets-datacollector-dev-lib"
	StageName                   = "com_streamsets_pipeline_stage_devtest_RandomDataGeneratorSource"
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
	EventName      string                `ConfigDef:"type=STRING,required=true"`
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

	for i := 0; i < maxBatchSize; i++ {
		rootField, _ := d.createField(r, delta, min)
		recordId := common.CreateRecordId("dev-data", i)
		if record, err := d.GetStageContext().CreateRecord(recordId, map[string]interface{}{}); err == nil {
			record.Set(rootField)
			batchMaker.AddRecord(record)
		} else {
			d.GetStageContext().ToError(err, record)
		}
	}

	rootEventField, _ := d.createField(r, delta, min)
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

func (d *Origin) createField(r *rand.Rand, delta int64, min int64) (*api.Field, error) {
	var rootField = make(map[string]*api.Field)
	for _, config := range d.DataGenConfigs {
		switch config.Type {
		case BOOLEAN:
			rootField[config.Field], _ = api.CreateBoolField(r.Int63()&(1<<62) == 0)
		case DATE:
			fallthrough
		case TIME:
			fallthrough
		case DATETIME:
			sec := r.Int63n(delta) + min
			rootField[config.Field], _ = api.CreateDateTimeField((time.Unix(sec, 0)))
		case DOUBLE:
			rootField[config.Field], _ = api.CreateDoubleField(r.Float64())
		case FLOAT:
			rootField[config.Field], _ = api.CreateFloatField(r.Float32())
		case INTEGER:
			rootField[config.Field], _ = api.CreateIntegerField(r.Int())
		case LONG:
			rootField[config.Field], _ = api.CreateLongField(r.Int63())
		case STRING:
			rootField[config.Field], _ = api.CreateStringField(fake.Sentence())
		case DECIMAL:
			i := new(big.Int)
			i.SetString("64443234234123423", 10)
			rootField[config.Field], _ = api.CreateBigIntField(*i)
		case ADDRESS_FULL_ADDRESS:
			rootField[config.Field], _ = api.CreateStringField(fake.StreetAddress())
		case ADDRESS_BUILDING_NUMBER:
			rootField[config.Field], _ = api.CreateStringField(fake.StreetAddress())
		case ADDRESS_STREET_ADDRESS:
			rootField[config.Field], _ = api.CreateStringField(fake.StreetAddress())
		case ADDRESS_CITY:
			rootField[config.Field], _ = api.CreateStringField(fake.City())
		case ADDRESS_STATE:
			rootField[config.Field], _ = api.CreateStringField(fake.State())
		case ADDRESS_COUNTRY:
			rootField[config.Field], _ = api.CreateStringField(fake.Country())
		case ADDRESS_LATITUDE:
			rootField[config.Field], _ = api.CreateFloatField(fake.Latitude())
		case ADDRESS_LONGITUDE:
			rootField[config.Field], _ = api.CreateFloatField(fake.Longitude())
		case APP_NAME:
			rootField[config.Field], _ = api.CreateStringField(fake.ProductName())
		case APP_AUTHOR:
			rootField[config.Field], _ = api.CreateStringField(fake.FullName())
		case APP_VERSION:
			rootField[config.Field], _ = api.CreateIntegerField(fake.MonthNum())
		case BUSINESS_CREDIT_CARD_NUMBER:
			rootField[config.Field], _ = api.CreateStringField(fake.CreditCardNum(fake.CreditCardType()))
		case BUSINESS_CREDIT_CARD_TYPE:
			rootField[config.Field], _ = api.CreateStringField(fake.CreditCardType())
		case COLOR:
			rootField[config.Field], _ = api.CreateStringField(fake.Color())
		case COMPANY_NAME:
			rootField[config.Field], _ = api.CreateStringField(fake.Company())
		case COMPANY_INDUSTRY:
			rootField[config.Field], _ = api.CreateStringField(fake.Industry())
		case COMPANY_BUZZWORD:
			rootField[config.Field], _ = api.CreateStringField(fake.ProductName())
		case COMPANY_URL:
			rootField[config.Field], _ = api.CreateStringField(fake.DomainName())
		case DEMOGRAPHIC:
			rootField[config.Field], _ = api.CreateStringField(fake.Characters())
		case EMAIL:
			rootField[config.Field], _ = api.CreateStringField(fake.EmailAddress())
		case FINANCE:
			rootField[config.Field], _ = api.CreateStringField(fake.Currency())
		case INTERNET:
			rootField[config.Field], _ = api.CreateStringField(fake.DomainName())
		case LOREM:
			rootField[config.Field], _ = api.CreateStringField(fake.Sentence())
		case NAME:
			rootField[config.Field], _ = api.CreateStringField(fake.FullName())
		case PHONENUMBER:
			rootField[config.Field], _ = api.CreateStringField(fake.Phone())
		case RACE:
			raceIndex := randIntRange(r, 0, len(race)-1)
			rootField[config.Field], _ = api.CreateStringField(race[raceIndex])
		case SEX:
			rootField[config.Field], _ = api.CreateStringField(fake.Gender())
		case SHAKESPEARE:
			rootField[config.Field], _ = api.CreateStringField(fake.Sentences())
		case SSN:
			rootField[config.Field], _ = api.CreateStringField(
				strconv.Itoa(randIntRange(r, 100000000, 999999999)),
			)
		case STOCK:
			rootField[config.Field], _ = api.CreateStringField(fake.CurrencyCode())
		default:
			rootField[config.Field], _ = api.CreateStringField(NotSupported)
		}
	}

	return api.CreateMapFieldWithMapOfFields(rootField), nil
}

func randIntRange(r *rand.Rand, min, max int) int {
	if min == max {
		return min
	}
	return r.Intn((max+1)-min) + min
}
