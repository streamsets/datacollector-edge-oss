package stagelibrary

import (
	"errors"
	"github.com/streamsets/sdc2go/api"
	"sync"
)

type NewStageCreator func() api.Stage

var reg *registry

type registry struct {
	sync.RWMutex
	newStageCreatorMap map[string]NewStageCreator
}

func init() {
	reg = new(registry)
	reg.newStageCreatorMap = make(map[string]NewStageCreator)
}

func SetCreator(library string, stageName string, newStageCreator NewStageCreator) {
	stageKey := library + ":" + stageName
	reg.Lock()
	reg.newStageCreatorMap[stageKey] = newStageCreator
	reg.Unlock()
}

func GetCreator(library string, stageName string) (NewStageCreator, bool) {
	stageKey := library + ":" + stageName
	reg.RLock()
	s, b := reg.newStageCreatorMap[stageKey]
	reg.RUnlock()
	return s, b
}

func CreateStageInstance(library string, stageName string) (api.Stage, error) {
	if t, ok := GetCreator(library, stageName); ok {
		v := t()
		return v, nil
	} else {
		return nil, errors.New("No Stage Instance found for : " + library + ", stage: " + stageName)
	}
}
