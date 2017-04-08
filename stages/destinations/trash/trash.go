package trash

import (
	"context"
	"github.com/streamsets/dataextractor/api"
)

type TrashDestination struct {
}

func (t *TrashDestination) Init(ctx context.Context) {

}

func (t *TrashDestination) Destroy() {

}

func (t *TrashDestination) Write(batch api.Batch) error {
	return nil
}
