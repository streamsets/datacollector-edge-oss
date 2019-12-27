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
	client "github.com/influxdata/influxdb1-client"
	"github.com/spf13/cast"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/util"
	"time"
)

const (
	Plugin          = "plugin"
	Time            = "time"
	TimeHires       = "time_hires"
	FieldPathPrefix = "/"
)

var TagFields = []string{
	"host",
	"plugin_instance",
	"instance",
	"type",
	"type_instance",
}

var NonValueFields = []string{
	Plugin,
	Time,
	TimeHires,
}

func (d *Destination) getCollectdPoint(record api.Record) (point client.Point, err error) {
	measurementField, err := getFieldValue(record, FieldPathPrefix+Plugin, true)
	if err != nil {
		return point, err
	}
	measurement := cast.ToString(measurementField.Value)

	precision := "ms"
	var pointTime time.Time
	var timeField *api.Field
	if timeField, err = record.Get(FieldPathPrefix + TimeHires); err != nil {
		return point, err
	}
	if timeField != nil && timeField.Value != nil {
		precision = "n"
		longValue := cast.ToInt64(timeField.Value)
		// https://collectd.org/wiki/index.php/High_resolution_time_format
		longValue = ((longValue >> 30) * 1000000000) + ((longValue&0x3FFFFFFF)<<30)/1000000000
		pointTime = time.Unix(longValue, 0)
	} else {
		if timeField, err = getFieldValue(record, FieldPathPrefix+Time, true); err != nil {
			return point, err
		}
		precision = "ms"
		pointTime = cast.ToTime(timeField.Value)
	}

	tags := make(map[string]string)
	for _, tagFieldName := range d.Conf.FieldMapping.TagFields {
		if tagField, err := record.Get(tagFieldName); err != nil {
			return point, err
		} else {
			tags[stripPathPrefix(tagFieldName)] = cast.ToString(tagField.Value)
		}
	}

	for _, tagFieldName := range TagFields {
		if tagField, err := getFieldValue(record, FieldPathPrefix+tagFieldName, true); err == nil {
			// To match the behavior of Influx's built-in collectd support we must rename this field.
			if tagFieldName == "plugin_instance" {
				tagFieldName = "instance"
			}
			tags[tagFieldName] = cast.ToString(tagField.Value)
		}
	}

	values := make(map[string]interface{})
	for fieldPath := range record.GetFieldPaths() {
		if d.isValueField(fieldPath) {
			if valueField, err := record.Get(fieldPath); err != nil {
				return point, err
			} else {
				values[stripPathPrefix(fieldPath)] = valueField.Value
			}
		}
	}

	point = client.Point{
		Measurement: measurement,
		Tags:        tags,
		Fields:      values,
		Time:        pointTime,
		Precision:   precision,
	}
	return
}

func (d *Destination) isValueField(fieldPath string) bool {
	fieldName := stripPathPrefix(fieldPath)
	if fieldName == "" || fieldName == FieldPathPrefix {
		return false
	}
	return util.IndexOf(fieldName, TagFields) == -1 && util.IndexOf(fieldName, NonValueFields) == -1 &&
		util.IndexOf(fieldPath, d.Conf.FieldMapping.TagFields) == -1
}
