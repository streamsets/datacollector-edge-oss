package trash

import (
	"github.com/streamsets/dataextractor/api"
	"github.com/streamsets/dataextractor/container/common"
)

type TrashDestination struct {
}

func (t *TrashDestination) Init(configuration common.StageConfiguration) {

}

func (t *TrashDestination) Destroy() {

}

func (t *TrashDestination) Write(batch api.Batch) error {
	return nil
}
