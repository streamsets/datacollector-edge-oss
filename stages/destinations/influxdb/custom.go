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
	"time"
)

func (d *Destination) getCustomMappingPoint(record api.Record) (point client.Point, err error) {
	measurementField, err := getFieldValue(record, d.Conf.FieldMapping.MeasurementField, true)
	if err != nil {
		return
	}
	measurement := cast.ToString(measurementField.Value)

	var pointTime time.Time
	if len(d.Conf.FieldMapping.TimeField) > 0 {
		if timeField, err := record.Get(d.Conf.FieldMapping.TimeField); err != nil {
			return point, err
		} else {
			pointTime = cast.ToTime(timeField.Value)
		}
	} else {
		pointTime = time.Now()
	}

	tags := make(map[string]string)
	for _, tagFieldName := range d.Conf.FieldMapping.TagFields {
		if tagField, err := record.Get(tagFieldName); err != nil {
			return point, err
		} else {
			tags[stripPathPrefix(tagFieldName)] = cast.ToString(tagField.Value)
		}
	}

	values := make(map[string]interface{})
	for _, valueFieldName := range d.Conf.FieldMapping.ValueFields {
		if valueField, err := record.Get(valueFieldName); err != nil {
			return point, err
		} else {
			values[stripPathPrefix(valueFieldName)] = valueField.Value
		}
	}

	precision := "s"
	if val, ok := MeasurementMap[d.Conf.FieldMapping.TimeUnit]; ok {
		precision = val
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
