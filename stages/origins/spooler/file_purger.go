// Copyright 2018 StreamSets Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package spooler

import (
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

type filePurger struct {
	archiveDir              string
	retentionTime           time.Duration
	destroyNotificationChan chan bool
}

func (f *filePurger) run() {
	ticker := time.NewTicker(1 * time.Minute)
	f.destroyNotificationChan = make(chan bool)
	go func() {
		for {
			select {
			case <-ticker.C:
				f.purge()
			case <-f.destroyNotificationChan:
				ticker.Stop()
				return
			}
		}
	}()
}

func (f *filePurger) purge() {
	fileInfo, err := ioutil.ReadDir(f.archiveDir)
	if err != nil {
		log.WithError(err).Error("failed to read archive directory")
		return
	}
	now := time.Now()
	for _, info := range fileInfo {
		if diff := now.Sub(info.ModTime()); diff > f.retentionTime {
			log.Debugf("Deleting archived file '%s', exceeded retention time %s", info.Name(), diff)
			if err := os.Remove(filepath.Join(f.archiveDir, info.Name())); err != nil {
				log.WithError(err).
					WithField("file", info.Name()).
					Error("Failed to delete file after retention time")
			}
		}
	}
}

func (f *filePurger) destroy() {
	f.destroyNotificationChan <- true
}

func NewFilePurger(conf SpoolDirConfigBean) *filePurger {
	return &filePurger{
		archiveDir:    conf.ArchiveDir,
		retentionTime: time.Duration(conf.RetentionTimeMins) * time.Minute,
	}
}
