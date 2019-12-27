// Copyright 2019 StreamSets Inc.
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
package influxdb

import (
	"fmt"
	_ "github.com/influxdata/influxdb1-client"
	client "github.com/influxdata/influxdb1-client"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"net/url"
	"strings"
)

const (
	Library   = "streamsets-datacollector-influxdb_0_9-lib"
	StageName = "com_streamsets_pipeline_stage_destination_influxdb_InfluxDTarget"
)

var MeasurementMap = map[string]string{
	"NANOSECONDS":  "n",
	"MICROSECONDS": "u",
	"MILLISECONDS": "ms",
	"SECONDS":      "s",
	"MINUTES":      "m",
	"HOURS":        "h",
	"DAYS":         "d",
}

type Destination struct {
	*common.BaseStage
	influxDBClient *client.Client
	Conf           InfluxConfigBean `ConfigDefBean:"conf"`
}

type InfluxConfigBean struct {
	Url                 string                           `ConfigDef:"type=STRING,required=true"`
	Username            string                           `ConfigDef:"type=STRING,required=true"`
	Password            string                           `ConfigDef:"type=STRING,required=true"`
	DbName              string                           `ConfigDef:"type=STRING,required=true"`
	AutoCreate          bool                             `ConfigDef:"type=BOOLEAN,required=true"`
	RetentionPolicy     string                           `ConfigDef:"type=STRING,required=true"`
	ConsistencyLevel    string                           `ConfigDef:"type=STRING,required=true"`
	RecordConverterType string                           `ConfigDef:"type=STRING,required=true"`
	FieldMapping        GenericRecordConverterConfigBean `ConfigDefBean:"fieldMapping"`
}

type GenericRecordConverterConfigBean struct {
	MeasurementField string   `ConfigDef:"type=STRING,required=true"`
	TimeField        string   `ConfigDef:"type=STRING,required=true"`
	TimeUnit         string   `ConfigDef:"type=STRING,required=true"`
	TagFields        []string `ConfigDef:"type=LIST,required=true"`
	ValueFields      []string `ConfigDef:"type=LIST,required=true"`
}

func init() {
	stagelibrary.SetCreator(Library, StageName, func() api.Stage {
		return &Destination{BaseStage: &common.BaseStage{}}
	})
}

func (d *Destination) Init(stageContext api.StageContext) []validation.Issue {
	issues := d.BaseStage.Init(stageContext)
	host, err := url.Parse(d.Conf.Url)
	if err != nil {
		issues = append(issues, stageContext.CreateConfigIssue(err.Error()))
		return issues
	}

	conf := client.Config{
		URL:              *host,
		Username:         d.Conf.Username,
		Password:         d.Conf.Password,
		WriteConsistency: d.Conf.ConsistencyLevel,
	}
	d.influxDBClient, err = client.NewClient(conf)
	if err != nil {
		issues = append(issues, stageContext.CreateConfigIssue(err.Error()))
		return issues
	}

	// Validate if URL is reachable
	_, _, err = d.influxDBClient.Ping()
	if err != nil {
		issues = append(issues, stageContext.CreateConfigIssue(err.Error()))
		return issues
	}
	return issues
}

func (d *Destination) Write(batch api.Batch) error {
	pts := d.getMappingPoints(batch)
	batchPoints := client.BatchPoints{
		Points:          pts,
		Database:        d.Conf.DbName,
		RetentionPolicy: d.Conf.RetentionPolicy,
	}
	_, err := d.influxDBClient.Write(batchPoints)
	if err != nil {
		d.GetStageContext().ReportError(err)
	}

	return nil
}

func (d *Destination) getMappingPoints(batch api.Batch) []client.Point {
	pts := make([]client.Point, 0)
	for _, record := range batch.GetRecords() {
		if point, err := d.getPoint(record); err != nil {
			d.GetStageContext().ToError(err, record)
		} else {
			pts = append(pts, point)
		}
	}
	return pts
}

func (d *Destination) getPoint(record api.Record) (point client.Point, err error) {
	if d.Conf.RecordConverterType == "COLLECTD" {
		return d.getCollectdPoint(record)
	} else {
		return d.getCustomMappingPoint(record)
	}
}

func stripPathPrefix(str string) string {
	lastIndex := strings.LastIndex(str, "/") + 1
	return str[lastIndex:]
}

func getFieldValue(record api.Record, fieldPath string, checkValForEmpty bool) (field *api.Field, err error) {
	field, err = record.Get(fieldPath)
	if checkValForEmpty && field != nil && field.Value == nil {
		err = fmt.Errorf("record is missing %s field", fieldPath)
	}
	return
}
