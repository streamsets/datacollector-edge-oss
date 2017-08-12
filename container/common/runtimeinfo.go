package common

import (
	"github.com/satori/go.uuid"
	"io/ioutil"
	"log"
	"os"
)

const (
	EDGE_ID_FILE = "/data/edge.id"
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
	var sdc2goId string
	if _, err := os.Stat(r.getSdeIdFilePath()); os.IsNotExist(err) {
		f, err := os.Create(r.getSdeIdFilePath())
		check(err)

		defer f.Close()
		sdc2goId = uuid.NewV4().String()
		f.WriteString(sdc2goId)
	} else {
		buf, err := ioutil.ReadFile(r.getSdeIdFilePath())
		if err != nil {
			log.Fatal(err)
		}
		sdc2goId = string(buf)
	}

	return sdc2goId
}

func (r *RuntimeInfo) getSdeIdFilePath() string {
	return r.BaseDir + EDGE_ID_FILE
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
