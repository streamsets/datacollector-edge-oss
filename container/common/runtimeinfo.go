package common

import (
	"github.com/satori/go.uuid"
	"io/ioutil"
	"log"
	"os"
)

const (
	SDE_ID_FILE = "/data/sde.id"
)

type RuntimeInfo struct {
	ID         string
	BaseDir    string
	HttpUrl    string
	DPMEnabled bool
}

func (r *RuntimeInfo) init() error {
	r.ID = r.getSdeId()
	return nil
}

func (r *RuntimeInfo) getSdeId() string {
	var sdeId string
	if _, err := os.Stat(r.getSdeIdFilePath()); os.IsNotExist(err) {
		f, err := os.Create(r.getSdeIdFilePath())
		check(err)

		defer f.Close()
		sdeId = uuid.NewV4().String()
		f.WriteString(sdeId)
	} else {
		buf, err := ioutil.ReadFile(r.getSdeIdFilePath())
		if err != nil {
			log.Fatal(err)
		}
		sdeId = string(buf)
	}

	return sdeId
}

func (r *RuntimeInfo) getSdeIdFilePath() string {
	return r.BaseDir + SDE_ID_FILE
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func NewRuntimeInfo(httpUrl string, baseDir string) (*RuntimeInfo, error) {
	runtimeInfo := RuntimeInfo{
		HttpUrl: httpUrl,
		BaseDir: baseDir,
	}
	err := runtimeInfo.init()
	if err != nil {
		return nil, err
	}
	return &runtimeInfo, nil
}
