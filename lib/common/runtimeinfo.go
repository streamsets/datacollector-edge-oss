package common

import (
	"github.com/satori/go.uuid"
	"io/ioutil"
	"log"
	"os"
)

const (
	SDE_ID_FILE = "data/sde.id"
)

type RuntimeInfo struct {
	ID         string
	HttpUrl    string
	DPMEnabled bool
	logger     *log.Logger
}

func (runtimeInfo *RuntimeInfo) init() error {
	runtimeInfo.ID = runtimeInfo.getSdeId()
	return nil
}

func (runtimeInfo *RuntimeInfo) getSdeId() string {
	var sdeId string
	if _, err := os.Stat(SDE_ID_FILE); os.IsNotExist(err) {
		f, err := os.Create(SDE_ID_FILE)
		check(err)

		defer f.Close()
		sdeId = uuid.NewV4().String()
		f.WriteString(sdeId)
	} else {
		buf, err := ioutil.ReadFile(SDE_ID_FILE)
		if err != nil {
			log.Fatal(err)
		}
		sdeId = string(buf)
	}

	return sdeId
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func NewRuntimeInfo(logger *log.Logger, httpUrl string) (*RuntimeInfo, error) {
	runtimeInfo := RuntimeInfo{logger: logger, HttpUrl: httpUrl}
	err := runtimeInfo.init()
	if err != nil {
		return nil, err
	}
	return &runtimeInfo, nil
}
