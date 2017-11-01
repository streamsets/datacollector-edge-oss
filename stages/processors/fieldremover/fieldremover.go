package fieldremover

import (
	"errors"
	"fmt"
	"github.com/streamsets/datacollector-edge/api"
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

func (f *FieldRemoverProcessor) Init(stageContext api.StageContext) error {
	if err := f.BaseStage.Init(stageContext); err != nil {
		return err
	}

	f.fieldList = make([]string, len(f.Fields))
	for i, field := range f.Fields {
		fieldPath, ok := field.(string)
		if !ok {
			return errors.New("Unexpected field list value")
		}
		f.fieldList[i] = fieldPath
	}

	if f.FilterOperation != KEEP && f.FilterOperation != REMOVE && f.FilterOperation != REMOVE_NULL {
		return errors.New("Unsupported field FilterOperation: " + f.FilterOperation)
	}
	return nil
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
