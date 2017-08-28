// +build 386 windows,amd64 windows

package windows

import (
	"github.com/streamsets/datacollector-edge/api"
	"github.com/streamsets/datacollector-edge/container/common"
	"github.com/streamsets/datacollector-edge/container/execution/runner"
	"github.com/streamsets/datacollector-edge/stages/stagelibrary"
	"testing"
)

func createStageContext(logName string) api.StageContext {
	stageConfig := common.StageConfiguration{}
	stageConfig.Library = LIBRARY
	stageConfig.StageName = STAGE_NAME
	stageConfig.Configuration = make([]common.Config, 2)

	stageConfig.Configuration[0] = common.Config{
		Name:  LOG_NAME_CONFIG,
		Value: logName,
	}
	stageConfig.Configuration[1] = common.Config{
		Name:  READ_MODE_CONFIG,
		Value: "ALL",
	}
	return &common.StageContextImpl{
		StageConfig: stageConfig,
		Parameters:  nil,
	}
}

func testWindowsEventLogRead(t *testing.T, logName string, maxBatchSize int) {
	stageInstance, err := stagelibrary.CreateStageInstance(LIBRARY, STAGE_NAME)
	if err != nil {
		t.Fatal(err)
	}

	stageContext := createStageContext(logName)
	err = stageInstance.Init(stageContext)

	if err != nil {
		t.Fatalf("Error when Initializing stage %s", err.Error())
	}

	defer stageInstance.Destroy()
	batchMaker := runner.NewBatchMakerImpl(runner.StagePipe{})

	_, err = stageInstance.(api.Origin).Produce("", maxBatchSize, batchMaker)

	if err != nil {
		t.Fatalf("Error when Producing %s", err.Error())
	}

	records := batchMaker.GetStageOutput()

	if len(records) <= 0 {
		t.Fatalf("Did not read any records")
	} else {
		for _, event := range records {
			rootField := event.Get().Value.(map[string]api.Field)
			actualLogName := rootField["LogName"].Value
			if actualLogName != logName {
				t.Fatalf("Wrong Log Name. Expected : %s, Actual : %s", logName, actualLogName)
			}
		}
	}
}

func TestWindowsApplicationLogRead(t *testing.T) {
	testWindowsEventLogRead(t, APPLICATION, 1)
}

func TestWindowsSystemLogRead(t *testing.T) {
	testWindowsEventLogRead(t, SYSTEM, 1)
}
