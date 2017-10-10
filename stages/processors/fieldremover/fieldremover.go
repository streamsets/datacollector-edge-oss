package fieldremover

import (
	"errors"
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/api/fieldtype"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"strings"
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
	for i, _ := range f.Fields {
		s, ok := f.Fields[i].(string)
		if !ok {
			return errors.New("Unexpected field list value")
		}
		f.fieldList[i] = strings.TrimPrefix(s, "/")
	}

	if f.FilterOperation != KEEP && f.FilterOperation != REMOVE && f.FilterOperation != REMOVE_NULL {
		return errors.New("Unsupported field FilterOperation: " + f.FilterOperation)
	}
	return nil
}

func (f *FieldRemoverProcessor) Process(batch api.Batch, batchMaker api.BatchMaker) error {
	for _, record := range batch.GetRecords() {
		rootField, err := record.Get()
		if err != nil {
			return err
		}
		rootFieldType := rootField.Type
		if rootFieldType == fieldtype.LIST_MAP || rootFieldType == fieldtype.MAP {
			recordFields := rootField.Value.(map[string]*api.Field)
			if f.FilterOperation == KEEP {
				field, err := api.CreateMapField(map[string]interface{}{})
				if err != nil {
					return err
				}
				record.Set(field)
				rootField, _ = record.Get()
			}
			for _, v := range f.fieldList {
				switch f.FilterOperation {
				case KEEP:
					if _, ok := recordFields[v]; ok {
						rootField.Value.(map[string]*api.Field)[v] = recordFields[v]
					}
				case REMOVE:
					delete(recordFields, v)
				case REMOVE_NULL:
					if _, ok := recordFields[v]; ok && recordFields[v].Value == "" {
						delete(recordFields, v)
					}
				}
			}
		}
		batchMaker.AddRecord(record)
	}
	return nil
}
