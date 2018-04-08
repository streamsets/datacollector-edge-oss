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
package fieldremover

import (
	"errors"
	"fmt"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/validation"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
)

const (
	LIBRARY         = "streamsets-datacollector-basic-lib"
	STAGE_NAME      = "com_streamsets_pipeline_stage_processor_fieldfilter_FieldFilterDProcessor"
	KEEP            = "KEEP"
	REMOVE          = "REMOVE"
	REMOVE_NULL     = "REMOVE_NULL"
	FIELDS          = "fields"
	FILTEROPERATION = "filterOperation"
	VERSION         = 1
)

type FieldRemoverProcessor struct {
	*common.BaseStage
	Fields          []interface{} `ConfigDef:"type=LIST,required=true"`
	FilterOperation string        `ConfigDef:"type=STRING,required=true"`
	fieldList       []string
}

func init() {
	stagelibrary.SetCreator(LIBRARY, STAGE_NAME, func() api.Stage {
		return &FieldRemoverProcessor{BaseStage: &common.BaseStage{}}
	})
}

func (f *FieldRemoverProcessor) Init(stageContext api.StageContext) []validation.Issue {
	issues := f.BaseStage.Init(stageContext)

	f.fieldList = make([]string, len(f.Fields))
	for i, field := range f.Fields {
		fieldPath, ok := field.(string)
		if !ok {
			issues = append(issues, stageContext.CreateConfigIssue("Unexpected field list value"))
			return issues
		}
		f.fieldList[i] = fieldPath
	}

	if f.FilterOperation != KEEP && f.FilterOperation != REMOVE && f.FilterOperation != REMOVE_NULL {
		issues = append(
			issues,
			stageContext.CreateConfigIssue("Unsupported field FilterOperation: "+f.FilterOperation),
		)
		return issues
	}
	return issues
}

func (f *FieldRemoverProcessor) Process(batch api.Batch, batchMaker api.BatchMaker) error {
	for _, record := range batch.GetRecords() {
		recordFieldPaths := record.GetFieldPaths()
		fieldsPathsToRemove := []string{}
		var err error
		switch f.FilterOperation {
		case REMOVE:
			fallthrough
		case REMOVE_NULL:
			for _, fieldToRemove := range f.fieldList {
				_, ok := recordFieldPaths[fieldToRemove]
				if ok {
					var recordField *api.Field
					recordField, err = record.Get(fieldToRemove)
					if err == nil {
						if f.FilterOperation == REMOVE || (f.FilterOperation == REMOVE_NULL && recordField.Value == "") {
							fieldsPathsToRemove = append(fieldsPathsToRemove, fieldToRemove)
						}
					}
				}
			}
		case KEEP:
			for _, fieldToKeep := range f.fieldList {
				delete(recordFieldPaths, fieldToKeep)
				for _, parentFieldPath := range f.getParentFields(fieldToKeep) {
					delete(recordFieldPaths, parentFieldPath)
				}
			}
			for fieldPathToRemove := range recordFieldPaths {
				fieldsPathsToRemove = append(fieldsPathsToRemove, fieldPathToRemove)
			}
		}

		if err == nil {
			for _, fieldPathToRemove := range fieldsPathsToRemove {
				_, err = record.Delete(fieldPathToRemove)
				if err != nil {
					err = errors.New(
						fmt.Sprintf("Error removing field : %s. Reason : %s", fieldPathToRemove, err.Error()))
					break
				}
			}
		}
		if err == nil {
			batchMaker.AddRecord(record)
		} else {
			f.GetStageContext().ToError(err, record)
		}
	}
	return nil
}

func (f *FieldRemoverProcessor) getParentFields(fieldPath string) []string {
	parentFields := []string{}
	for index := 0; index < len(fieldPath); {
		c := fieldPath[index]
		switch c {
		case '/':
			fallthrough
		case '[':
			parentFields = append(parentFields, fieldPath[:index])
			break
		default:
			break
		}
		index++
	}
	return parentFields
}
