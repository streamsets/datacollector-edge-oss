package dev_random

import (
	"github.com/jmcvetta/randutil"
	"github.com/streamsets/dataextractor/api"
	"github.com/streamsets/dataextractor/container/common"
	"github.com/streamsets/dataextractor/stages/stagelibrary"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

const (
	LIBRARY    = "streamsets-datacollector-dev-lib"
	STAGE_NAME = "com_streamsets_pipeline_stage_devtest_RandomSource"
	DEMO_DATA  = true
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

func (d DevRandom) Init(stageContext api.StageContext) error {
	if err := d.BaseStage.Init(stageContext); err != nil {
		return err
	}
	stageConfig := d.GetStageConfig()
	for _, config := range stageConfig.Configuration {
		if config.Name == "fields" {
			d.fields = strings.SplitAfter(config.Value.(string), ",")
		} else if config.Name == "delay" {
			d.delay = config.Value.(float64)
		}
	}
	return nil
}

func (d DevRandom) Produce(lastSourceOffset string, maxBatchSize int, batchMaker api.BatchMaker) (string, error) {
	if DEMO_DATA {
		d.produceTestDataForDemo(maxBatchSize, batchMaker)
	} else {
		r := rand.New(rand.NewSource(99))

		time.Sleep(time.Duration(d.delay) * time.Millisecond)

		for i := 0; i < maxBatchSize; i++ {
			var recordValue = make(map[string]int)
			for _, field := range d.fields {
				recordValue[field] = r.Int()
			}
			batchMaker.AddRecord(d.GetStageContext().CreateRecord("dev-random", recordValue))
		}
	}

	return "random", nil
}

// Sample code for demo purpose
// bin/dataextractor -start=randomToHttp -runtimeParameters='{"httpUrl":"http://localhost:9999","sdcAppId":"sdc2go"}'

type TestData struct {
	Firmware         string                 `json:"firmware"`
	DeviceId         int                    `json:"device_id"`
	ContainerId      string                 `json:"container_id"`
	ReadingDate      string                 `json:"reading_date"`
	Temperature      string                 `json:"temperature"`
	TempUnit         string                 `json:"temp_unit"`
	RelativeHumidity string                 `json:"relative_humidity"`
	Orientation      map[string]interface{} `json:"orientation"`
	Location         map[string]interface{} `json:"location"`
}

func (d DevRandom) produceTestDataForDemo(maxBatchSize int, batchMaker api.BatchMaker) {
	time.Sleep(time.Duration(d.delay) * time.Millisecond)
	for i := 0; i < maxBatchSize; i++ {
		batchMaker.AddRecord(
			d.GetStageContext().CreateRecord(
				"dev-random",
				getData()))
	}
}

func getData() TestData {
	deviceId, _ := randutil.IntRange(20000, 40000)
	tempUnit := "C"

	containerIdChoices := make([]randutil.Choice, 0, 5)
	containerIdChoices = append(containerIdChoices, randutil.Choice{8, "efe755d7"})
	containerIdChoices = append(containerIdChoices, randutil.Choice{8, "4b5f16a1"})
	containerIdChoices = append(containerIdChoices, randutil.Choice{7, "5ed2f15c"})
	containerIdChoices = append(containerIdChoices, randutil.Choice{6, "eadd05fe"})
	containerIdChoices = append(containerIdChoices, randutil.Choice{5, "30ab0bfd"})
	containerId, _ := randutil.WeightedChoice(containerIdChoices)

	var firmware string
	switch containerId.Item.(string) {
	case "efe755d7":
		firmware = "1.0"
		break
	case "4b5f16a1":
		firmware = "1.0"
		break
	case "5ed2f15c":
		firmware = "1.0"
		break
	case "eadd05fe":
		firmware = "2.0"
		break
	case "30ab0bfd":
		firmware = "3.0"
		break
	default:
		firmware = "1.0"
	}

	tempChoices := make([]randutil.Choice, 0, 10)
	tempChoices = append(tempChoices, randutil.Choice{8, "0"})
	tempChoices = append(tempChoices, randutil.Choice{8, "0.5"})
	tempChoices = append(tempChoices, randutil.Choice{7, "1.0"})
	tempChoices = append(tempChoices, randutil.Choice{6, "1.5"})
	tempChoices = append(tempChoices, randutil.Choice{5, "2.0"})
	tempChoices = append(tempChoices, randutil.Choice{4, "2.5"})
	tempChoices = append(tempChoices, randutil.Choice{7 / 10, "3.0"})
	tempChoices = append(tempChoices, randutil.Choice{4 / 10, "3.5"})
	tempChoices = append(tempChoices, randutil.Choice{1 / 10, "8"})
	tempChoices = append(tempChoices, randutil.Choice{5 / 10, "FFF"})
	temperature, _ := randutil.WeightedChoice(tempChoices)

	// var rh = chance.weighted(['50', '51', '52', '53', '54', '55', '60', '70', '90'], [8,8,7,6,5,4,0.7,0.4, 0.1]);
	relativeHumidityChoices := make([]randutil.Choice, 0, 10)
	relativeHumidityChoices = append(relativeHumidityChoices, randutil.Choice{8, "50"})
	relativeHumidityChoices = append(relativeHumidityChoices, randutil.Choice{8, "51"})
	relativeHumidityChoices = append(relativeHumidityChoices, randutil.Choice{7, "52"})
	relativeHumidityChoices = append(relativeHumidityChoices, randutil.Choice{6, "53"})
	relativeHumidityChoices = append(relativeHumidityChoices, randutil.Choice{5, "54"})
	relativeHumidityChoices = append(relativeHumidityChoices, randutil.Choice{4, "55"})
	relativeHumidityChoices = append(relativeHumidityChoices, randutil.Choice{7 / 10, "60"})
	relativeHumidityChoices = append(relativeHumidityChoices, randutil.Choice{4 / 10, "70"})
	relativeHumidityChoices = append(relativeHumidityChoices, randutil.Choice{1 / 10, "90"})
	relativeHumidity, _ := randutil.WeightedChoice(relativeHumidityChoices)

	var testData TestData
	switch firmware {
	case "1.0":
		testData = TestData{
			Firmware:         firmware,
			DeviceId:         deviceId,
			ContainerId:      containerId.Item.(string),
			ReadingDate:      strconv.FormatInt(time.Now().Unix(), 10),
			Temperature:      temperature.Item.(string),
			TempUnit:         tempUnit,
			RelativeHumidity: relativeHumidity.Item.(string),
		}
		break
	case "2.0":
		testData = TestData{
			Firmware:         firmware,
			DeviceId:         deviceId,
			ContainerId:      containerId.Item.(string),
			ReadingDate:      strconv.FormatInt(time.Now().Unix(), 10),
			Temperature:      temperature.Item.(string),
			TempUnit:         tempUnit,
			RelativeHumidity: relativeHumidity.Item.(string),
			Orientation:      getOrientation(),
		}
		break
	case "3.0":
		testData = TestData{
			Firmware:         firmware,
			DeviceId:         deviceId,
			ContainerId:      containerId.Item.(string),
			ReadingDate:      strconv.FormatInt(time.Now().Unix(), 10),
			Temperature:      temperature.Item.(string),
			TempUnit:         tempUnit,
			RelativeHumidity: relativeHumidity.Item.(string),
			Orientation:      getOrientation(),
			Location:         getLocation(),
		}
		break
	case "1.3A":
	default:
		testData = TestData{
			Firmware:         firmware,
			DeviceId:         deviceId,
			ContainerId:      containerId.Item.(string),
			ReadingDate:      strconv.FormatInt(time.Now().Unix(), 10),
			Temperature:      temperature.Item.(string),
			TempUnit:         tempUnit,
			RelativeHumidity: relativeHumidity.Item.(string),
		}
	}

	return testData
}

func getOrientation() map[string]interface{} {
	rollChoices := make([]randutil.Choice, 0, 10)
	rollChoices = append(rollChoices, randutil.Choice{4, -10})
	rollChoices = append(rollChoices, randutil.Choice{5, -9})
	rollChoices = append(rollChoices, randutil.Choice{7, -5})
	rollChoices = append(rollChoices, randutil.Choice{6, -2})
	rollChoices = append(rollChoices, randutil.Choice{8, 0})
	rollChoices = append(rollChoices, randutil.Choice{4, 3})
	rollChoices = append(rollChoices, randutil.Choice{7, 7})
	rollChoices = append(rollChoices, randutil.Choice{4 / 10, 12})
	rollChoices = append(rollChoices, randutil.Choice{1 / 10, 90})
	roll, _ := randutil.WeightedChoice(rollChoices)

	pitchChoices := make([]randutil.Choice, 0, 10)
	pitchChoices = append(pitchChoices, randutil.Choice{4, -12})
	pitchChoices = append(pitchChoices, randutil.Choice{5, -8})
	pitchChoices = append(pitchChoices, randutil.Choice{7, -5})
	pitchChoices = append(pitchChoices, randutil.Choice{6, -1})
	pitchChoices = append(pitchChoices, randutil.Choice{8, 0})
	pitchChoices = append(pitchChoices, randutil.Choice{4, 4})
	pitchChoices = append(pitchChoices, randutil.Choice{7, 9})
	pitchChoices = append(pitchChoices, randutil.Choice{4 / 10, 12})
	pitchChoices = append(pitchChoices, randutil.Choice{1 / 10, 60})
	pitch, _ := randutil.WeightedChoice(pitchChoices)

	yawChoices := make([]randutil.Choice, 0, 10)
	yawChoices = append(yawChoices, randutil.Choice{4, -180})
	yawChoices = append(yawChoices, randutil.Choice{5, -120})
	yawChoices = append(yawChoices, randutil.Choice{7, -50})
	yawChoices = append(yawChoices, randutil.Choice{6, 10})
	yawChoices = append(yawChoices, randutil.Choice{8, 30})
	yawChoices = append(yawChoices, randutil.Choice{4, 40})
	yawChoices = append(yawChoices, randutil.Choice{7, 100})
	yawChoices = append(yawChoices, randutil.Choice{4 / 10, 120})
	yawChoices = append(yawChoices, randutil.Choice{1 / 10, 160})
	yaw, _ := randutil.WeightedChoice(yawChoices)

	orientation := make(map[string]interface{})
	orientation["roll"] = roll.Item.(int)
	orientation["pitch"] = pitch.Item.(int)
	orientation["yaw"] = yaw

	return orientation
}

func getLocation() map[string]interface{} {
	location := make(map[string]interface{})
	location["lat"], _ = randutil.IntRange(35, 47)
	location["long"], _ = randutil.IntRange(70, 125)
	return location
}
