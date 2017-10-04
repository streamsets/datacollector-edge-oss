package fieldremover

import (
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/creation"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"strings"
	"testing"
)

func getStageContext(fields []interface{}, filterOperation string, parameters map[string]interface{}) *common.StageContextImpl {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = LIBRARY
	stageConfig.StageName = STAGE_NAME
	stageConfig.Configuration = make([]common.Config, 2)
	stageConfig.Configuration[0] = common.Config{
		Name:  FIELDS,
		Value: fields,
	}
	stageConfig.Configuration[1] = common.Config{
		Name:  FILTEROPERATION,
		Value: filterOperation,
	}
	return &common.StageContextImpl{
		StageConfig: stageConfig,
		Parameters:  parameters,
	}
}

func TestFieldRemoverProcessor_Init(t *testing.T) {
	fields := []interface{}{"/a", "/b", "/c"}
	filterOperation := REMOVE
	stageContext := getStageContext(fields, filterOperation, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage

	if stageInstance.(*FieldRemoverProcessor).Fields == nil {
		t.Error("Failed to inject config value for Fields")
	}
}

func TestFieldRemoverProcessor_InitUnsupported(t *testing.T) {
	fields := []interface{}{"/a", "/b", "/c"}
	filterOperation := "SOMEFILTER"
	stageContext := getStageContext(fields, filterOperation, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	err = stageInstance.Init(stageContext)
	if err == nil || !strings.Contains(err.Error(), "Unsupported") {
		t.Error("Filter operation not properly flagged as unsupported")
	}
}

func TestFieldRemoverProcessor_InitUnexpected(t *testing.T) {
	fields := []interface{}{"/a", 11, "/c"}
	filterOperation := "KEEP"
	stageContext := getStageContext(fields, filterOperation, nil)
	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage
	err = stageInstance.Init(stageContext)
	if err == nil || !strings.Contains(err.Error(), "Unexpected") {
		t.Error("Fields list integer not properly flagged as unexpected")
	}
}

func TestFieldRemoverProcessorRemove(t *testing.T) {
	fields := []interface{}{"/a", "/b", "/c"}
	filterOperation := REMOVE
	stageContext := getStageContext(fields, filterOperation, nil)

	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage

	err = stageInstance.Init(stageContext)
	if err != nil {
		t.Error(err)
	}

	records := make([]api.Record, 3)
	records[0], _ = stageContext.CreateRecord("0", map[string]interface{}{"a": 123, "b": 456, "d": 78})
	records[1], _ = stageContext.CreateRecord("1", map[string]interface{}{"b": 456, "d": 78, "g": "9"})
	records[2], _ = stageContext.CreateRecord("2", map[string]interface{}{"x": nil, "y": 3e2, "z": 'a'})
	batch := runner.NewBatchImpl("fieldRemover", records, "randomOffset")
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{})

	err = stageInstance.(api.Processor).Process(batch, batchMaker)
	if err != nil {
		t.Error("Error in Identity Processor")
	}

	var field api.Field
	field, _ = batchMaker.GetStageOutput()[0].Get()
	if len(field.Value.(map[string]api.Field)) != 1 {
		t.Error("Fields not removed properly")
	}

	field, _ = batchMaker.GetStageOutput()[1].Get()
	if len(field.Value.(map[string]api.Field)) != 2 {
		t.Error("Fields not removed properly")
	}

	field, _ = batchMaker.GetStageOutput()[2].Get()
	if len(field.Value.(map[string]api.Field)) != 3 {
		t.Error("Fields not removed properly")
	}

	stageInstance.Destroy()
}

func TestFieldRemoverProcessorKeep(t *testing.T) {
	fields := []interface{}{"/a", "/b", "/c"}
	filterOperation := KEEP
	stageContext := getStageContext(fields, filterOperation, nil)

	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage

	err = stageInstance.Init(stageContext)
	if err != nil {
		t.Error(err)
	}

	records := make([]api.Record, 1)
	records[0], _ = stageContext.CreateRecord("1", map[string]interface{}{"a": 123, "b": 456, "d": 78})
	batch := runner.NewBatchImpl("fieldRemover", records, "randomOffset")
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{})

	err = stageInstance.(api.Processor).Process(batch, batchMaker)
	if err != nil {
		t.Error("Error in Identity Processor")
	}

	field, _ := batchMaker.GetStageOutput()[0].Get()
	if len(field.Value.(map[string]api.Field)) != 2 {
		t.Error("Fields not removed properly")
	}

	stageInstance.Destroy()
}

func TestFieldRemoverProcessorRemoveNull(t *testing.T) {
	fields := []interface{}{"/a", "/b", "/c"}
	filterOperation := REMOVE_NULL
	stageContext := getStageContext(fields, filterOperation, nil)

	stageBean, err := creation.NewStageBean(stageContext.StageConfig, stageContext.Parameters)
	if err != nil {
		t.Error(err)
	}
	stageInstance := stageBean.Stage

	err = stageInstance.Init(stageContext)
	if err != nil {
		t.Error(err)
	}

	records := make([]api.Record, 1)
	records[0], _ = stageContext.CreateRecord("1", map[string]interface{}{"a": 123, "b": 456, "d": 78, "c": nil, "g": nil})
	batch := runner.NewBatchImpl("fieldRemover", records, "randomOffset")
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{})

	err = stageInstance.(api.Processor).Process(batch, batchMaker)
	if err != nil {
		t.Error("Error in Identity Processor")
	}

	field, _ := batchMaker.GetStageOutput()[0].Get()
	if len(field.Value.(map[string]api.Field)) != 4 {
		t.Error("Fields not removed properly")
	}

	stageInstance.Destroy()
}
