// +build arm,linux

/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Development Only Origin
// Supported only for Linux ARM

package sensor_reader

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"periph.io/x/periph/conn/i2c"
	"periph.io/x/periph/conn/i2c/i2creg"
	"periph.io/x/periph/devices"
	"periph.io/x/periph/devices/bmxx80"
	"periph.io/x/periph/host"
	"strconv"
	"time"
)

const (
	LIBRARY    = "streamsets-datacollector-dev-lib"
	STAGE_NAME = "com_streamsets_pipeline_stage_origin_sensorreader_SensorReaderDSource"
)

type SensorReaderOrigin struct {
	*common.BaseStage
	Conf SensorReaderConfigBean `ConfigDefBean:"name=conf"`
	bus  i2c.BusCloser
	dev  *bmxx80.Dev
}

type SensorReaderConfigBean struct {
	SensorDevice string  `ConfigDef:"type=STRING,required=true"`
	I2cAddress   string  `ConfigDef:"type=STRING,required=true"`
	Delay        float64 `ConfigDef:"type=NUMBER,required=true"`
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &SensorReaderOrigin{BaseStage: &common.BaseStage{}}
	})
}

func (s *SensorReaderOrigin) Init(stageContext api.StageContext) error {
	var err error
	if err := s.BaseStage.Init(stageContext); err != nil {
		return err
	}

	// Currently only Sensor device supported value is BMxx80
	if s.Conf.SensorDevice != "BMxx80" {
		return errors.New(fmt.Sprintf("Not supported reading from device: %s", s.Conf.SensorDevice))
	}

	i2cAddressHex, err := strconv.ParseUint(s.Conf.I2cAddress, 0, 16)

	if _, err := host.Init(); err != nil {
		return err
	}

	// Open a handle to the first available I²C bus:
	s.bus, err = i2creg.Open("")
	if err != nil {
		return err
	}

	// Open a handle to a bme280/bmp280 connected on the I²C bus using default
	// settings:
	s.dev, err = bmxx80.NewI2C(s.bus, uint16(i2cAddressHex), nil)
	if err != nil {
		return err
	}

	return nil
}

func (s *SensorReaderOrigin) Produce(
	lastSourceOffset string,
	maxBatchSize int,
	batchMaker api.BatchMaker,
) (string, error) {
	time.Sleep(time.Duration(s.Conf.Delay) * time.Millisecond)
	if s.dev != nil {
		var err error
		var env devices.Environment
		if err = s.dev.Sense(&env); err != nil {
			log.WithError(err).Error("Failed to read data from sensor")
			return "", err
		}
		fmt.Printf("%8s %10s %9s\n", env.Temperature, env.Pressure, env.Humidity)

		var recordValue = make(map[string]interface{})
		recordValue["temperature_C"] = env.Temperature.Float64()
		recordValue["pressure_KPa"] = env.Pressure.Float64()
		recordValue["humidity"] = env.Humidity.Float64()
		if record, err := s.GetStageContext().CreateRecord("sensorReader", recordValue); err == nil {
			batchMaker.AddRecord(record)
		} else {
			s.GetStageContext().ToError(err, record)
		}
	}
	return "sensorReader", nil
}

func (s *SensorReaderOrigin) Destroy() error {
	s.bus.Close()
	s.dev.Halt()
	return nil
}
