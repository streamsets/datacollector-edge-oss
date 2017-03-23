package stagelibrary

import (
	"github.com/streamsets/dataextractor/api"
	"github.com/streamsets/dataextractor/stages/destinations/http"
	"github.com/streamsets/dataextractor/stages/destinations/trash"
	"github.com/streamsets/dataextractor/stages/origins/dev_random"
	"github.com/streamsets/dataextractor/stages/origins/filetail"
)

func CreateStageInstance(library string, name string) api.Stage {
	var instanceKey = library + ":" + name
	switch instanceKey {
	case "streamsets-datacollector-dev-lib:com_streamsets_pipeline_stage_devtest_RandomSource":
		return &dev_random.DevRandom{}
	case "streamsets-datacollector-basic-lib:com_streamsets_pipeline_stage_origin_logtail_FileTailDSource":
		return &filetail.FileTailOrigin{}
	case "streamsets-datacollector-basic-lib:com_streamsets_pipeline_stage_destination_http_HttpClientDTarget":
		return &http.HttpClientDestination{}
	case "streamsets-datacollector-basic-lib:com_streamsets_pipeline_stage_destination_devnull_NullDTarget":
		return &trash.TrashDestination{}
	case "streamsets-datacollector-basic-lib:com_streamsets_pipeline_stage_destination_devnull_ToErrorNullDTarget":
		return &trash.TrashDestination{}
	}

	panic("No Stage Instance found for : " + instanceKey)
}
