// +build tensorflow

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

package tensorflow

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	tf "github.com/tensorflow/tensorflow/tensorflow/go"
)

const (
	Library             = "streamsets-datacollector-tensorflow-lib"
	StageName           = "com_streamsets_pipeline_stage_processor_tensorflow_TensorFlowDProcessor"
	ConfGroupTensorFlow = "TENSOR_FLOW"
	ConfModelPath       = "conf.modelPath"
)

type Processor struct {
	*common.BaseStage
	Conf            ProcessorConfigBean `ConfigDefBean:"conf"`
	tfSavedModel    *tf.SavedModel
	feedsOutputList []tf.Output
	fetches         []tf.Output
}

type ProcessorConfigBean struct {
	ModelPath      string               `ConfigDef:"type=STRING,required=true"`
	ModelTags      []string             `ConfigDef:"type=LIST,required=true"`
	UseEntireBatch bool                 `ConfigDef:"type=BOOLEAN,required=true"`
	OutputField    string               `ConfigDef:"type=STRING,required=true"`
	InputConfigs   []TensorInputConfig  `ConfigDef:"type=MODEL" ListBeanModel:"name=inputConfigs"`
	OutputConfigs  []TensorOutputConfig `ConfigDef:"type=MODEL" ListBeanModel:"name=outputConfigs"`
}

type TensorInputConfig struct {
	Operation      string    `ConfigDef:"type=STRING,required=true"`
	Index          float64   `ConfigDef:"type=NUMBER,required=true"`
	TensorDataType string    `ConfigDef:"type=STRING,required=true"`
	Fields         []string  `ConfigDef:"type=LIST,required=true"`
	Shape          []float64 `ConfigDef:"type=LIST,required=true"`
}

type TensorOutputConfig struct {
	Operation      string  `ConfigDef:"type=STRING,required=true"`
	Index          float64 `ConfigDef:"type=NUMBER,required=true"`
	TensorDataType string  `ConfigDef:"type=STRING,required=true"`
}

func init() {
	stagelibrary.SetCreator(Library, StageName, func() api.Stage {
		return &Processor{BaseStage: &common.BaseStage{}}
	})
}

func (p *Processor) Init(stageContext api.StageContext) []validation.Issue {
	issues := p.BaseStage.Init(stageContext)
	var err error
	p.tfSavedModel, err = tf.LoadSavedModel(p.Conf.ModelPath, p.Conf.ModelTags, nil)
	if err != nil {
		log.WithError(err).Error("Error loading saved model")
		issues = append(issues, stageContext.CreateConfigIssue(
			fmt.Sprintf("Error loading saved model: %s", err.Error()),
			ConfGroupTensorFlow,
			ConfModelPath,
		))
		return issues
	}

	p.feedsOutputList = make([]tf.Output, len(p.Conf.InputConfigs))
	for i, inputConfig := range p.Conf.InputConfigs {
		p.feedsOutputList[i] = p.tfSavedModel.Graph.Operation(inputConfig.Operation).Output(int(inputConfig.Index))
	}

	p.fetches = make([]tf.Output, len(p.Conf.OutputConfigs))
	for i, outputConfig := range p.Conf.OutputConfigs {
		p.fetches[i] = p.tfSavedModel.Graph.Operation(outputConfig.Operation).Output(int(outputConfig.Index))
	}

	return issues
}

func (p *Processor) Process(batch api.Batch, batchMaker api.BatchMaker) error {
	if p.Conf.UseEntireBatch {
		return p.processEntireBatch(batch, batchMaker)
	} else {
		return p.processRecordByRecord(batch, batchMaker)
	}
}

func (p *Processor) processRecordByRecord(batch api.Batch, batchMaker api.BatchMaker) error {

	for _, record := range batch.GetRecords() {
		var err error
		feeds := make(map[tf.Output]*tf.Tensor)

		for i, inputConfig := range p.Conf.InputConfigs {
			var tensor *tf.Tensor
			inputTfOp := p.tfSavedModel.Graph.Operation(inputConfig.Operation)
			tensor, err = ConvertFieldToTensor(record, inputConfig, inputTfOp)
			if err != nil {
				log.WithError(err).Error("Failed to create new tensor")
				break
			}
			feeds[p.feedsOutputList[i]] = tensor
		}

		if err != nil {
			p.GetStageContext().ToError(err, record)
			break
		}

		result, err := p.tfSavedModel.Session.Run(feeds, p.fetches, nil)
		if err != nil {
			fmt.Printf("Error running the session with input, err: %s\n", err.Error())
			p.GetStageContext().ToError(err, record)
			break
		}

		outputTensorFieldMap := make(map[string]*api.Field)
		for i, outputConfig := range p.Conf.OutputConfigs {
			fieldPath := outputConfig.Operation + "_" + fmt.Sprintf("%.0f", outputConfig.Index)
			var field *api.Field
			field, err = ConvertTensorToField(result[i])
			if err != nil {
				fmt.Printf("Error creating output field, err: %s\n", err.Error())
				break
			}
			outputTensorFieldMap[fieldPath] = field
		}

		if err != nil {
			p.GetStageContext().ToError(err, record)
			break
		}

		mapField := api.CreateMapFieldWithMapOfFields(outputTensorFieldMap)
		record.SetField(p.Conf.OutputField, mapField)

		batchMaker.AddRecord(record)
	}

	return nil
}

func (p *Processor) processEntireBatch(batch api.Batch, batchMaker api.BatchMaker) error {
	// TODO: Add Support for Entire batch once event lanes support is added
	return nil
}

func (p *Processor) Destroy() error {
	if p.tfSavedModel != nil {
		err := p.tfSavedModel.Session.Close()
		if err != nil {
			log.WithError(err).Error("Failed to close TensorFlow Session")
			return err
		}
	}
	return p.BaseStage.Destroy()
}
