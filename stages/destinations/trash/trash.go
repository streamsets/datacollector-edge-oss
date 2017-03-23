package trash

import "github.com/streamsets/dataextractor/api"

type TrashDestination struct {
}

func (t *TrashDestination) Init() {

}

func (t *TrashDestination) Destroy() {

}

func (t *TrashDestination) Write(batch api.Batch) error {
	return nil
}
