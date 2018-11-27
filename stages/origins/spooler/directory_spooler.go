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
	"github.com/streamsets/datacollector-edge/api"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type DirectorySpooler struct {
	currentFileInfo         *AtomicFileInformation
	dirPath                 string
	processSubDirs          bool
	maxNumberOfFiles        int
	destroyNotificationChan chan bool
	filesQueue              *SynchronizedFilesHeap
	spoolingPeriodDuration  time.Duration
	poolingTimeoutDuration  time.Duration
	readOrder               string
	filePattern             string
	pathMatcherMode         string
	currentFileChange       chan *AtomicFileInformation
	stageContext            api.StageContext
}

func isFileEligible(
	path string,
	modTime time.Time,
	currentFileInformation *AtomicFileInformation,
	readOrder string,
) bool {
	if currentFileInformation != nil {
		return (readOrder == Timestamp &&
			(modTime.After(currentFileInformation.getModTime()) ||
				(modTime.Equal(currentFileInformation.getModTime()) &&
					strings.Compare(path, currentFileInformation.getFullPath()) > 0))) ||
			(readOrder == Lexicographical &&
				(strings.Compare(path, currentFileInformation.getFullPath()) > 0 ||
					(strings.Compare(path, currentFileInformation.getFullPath()) == 0 &&
						modTime.After(currentFileInformation.getModTime()))))
	}
	return true
}

func isMatch(pathMatcherMode string, fileName string, filePattern string) (bool, error) {
	if pathMatcherMode == Glob {
		return filepath.Match(filePattern, fileName)
	} else {
		return regexp.MatchString(filePattern, fileName)
	}
}

func (d *DirectorySpooler) findAndAddMatchingFilesInTheDirectory() error {
	if d.pathMatcherMode == Regex {
		allFileInfos, err := ioutil.ReadDir(d.dirPath)
		if err == nil {
			for _, fileInfo := range allFileInfos {
				if matched, err := isMatch(d.pathMatcherMode, fileInfo.Name(), d.filePattern); err == nil && matched {
					d.addPathToQueueIfEligible(
						d.dirPath+"/"+fileInfo.Name(),
						fileInfo.ModTime(),
						d.currentFileInfo,
					)
				}
			}
		}
		return err
	} else {
		filePaths, err := filepath.Glob(d.dirPath + "/" + d.filePattern)
		if err == nil {
			for _, fileMatch := range filePaths {
				fileInfo, err := os.Stat(fileMatch)
				if err != nil {
					return err
				}
				d.addPathToQueueIfEligible(
					d.dirPath+"/"+fileInfo.Name(),
					fileInfo.ModTime(),
					d.currentFileInfo,
				)
			}
		}
		return err
	}
}

func (d *DirectorySpooler) addPathToQueueIfEligible(
	path string,
	modTime time.Time,
	currentFileInfo *AtomicFileInformation,
) {
	if isFileEligible(path, modTime, currentFileInfo, d.readOrder) {
		fileInfo := NewAtomicFileInformation(path, modTime, 0)
		if !d.filesQueue.Contains(fileInfo.getFullPath()) {
			log.WithField("offset", fileInfo.createOffset()).Debug("Pushing offset to queue")
			d.filesQueue.Push(fileInfo)
		}
	} else {
		log.WithField("ignored", path).Debug("File ignored because it is not eligible")
	}
}

func (d *DirectorySpooler) walkDirectoryPath(currentFileInfo *AtomicFileInformation) error {
	log.Debug("Spooler Starting")
	if d.processSubDirs {
		return filepath.Walk(d.dirPath, func(path string, info os.FileInfo, err error) error {
			if err == nil && path != d.dirPath {
				if !info.IsDir() {
					if matched, err := isMatch(d.pathMatcherMode, info.Name(), d.filePattern); err == nil && matched {
						d.addPathToQueueIfEligible(path, info.ModTime(), currentFileInfo)
					}
				}
			}
			return err
		})
	}
	return d.findAndAddMatchingFilesInTheDirectory()
}

func (d *DirectorySpooler) Init() {
	d.destroyNotificationChan = make(chan bool)
	d.filesQueue = NewSynchronizedFilesHeap(d.readOrder)
	d.currentFileChange = make(chan *AtomicFileInformation)
	d.currentFileInfo = nil
	if strings.HasSuffix(d.dirPath, "/") {
		d.dirPath = strings.TrimRight(d.dirPath, "/")
	}
	//Starting Spooler immediately and after that at regular intervals
	_ = d.walkDirectoryPath(d.currentFileInfo)
	go func(currentFileInfo *AtomicFileInformation) {
		end := false
		for !end {
			select {
			case <-time.After(d.spoolingPeriodDuration):
				_ = d.walkDirectoryPath(currentFileInfo)
			case fInfo := <-d.currentFileChange:
				currentFileInfo = fInfo
			case <-d.destroyNotificationChan:
				end = true
			}
		}
	}(d.currentFileInfo)
}

func (d *DirectorySpooler) setCurrentFileInfo(atf *AtomicFileInformation) {
	d.currentFileInfo = atf
	d.currentFileChange <- d.currentFileInfo
}

func (d *DirectorySpooler) getCurrentFileInfo() *AtomicFileInformation {
	return d.currentFileInfo
}

func (d *DirectorySpooler) NextFile() *AtomicFileInformation {
	initial := time.Now()
	var fi *AtomicFileInformation
	log.Debugf("polling for file, waiting '%d' ", d.poolingTimeoutDuration)
	for fi == nil && time.Since(initial) < d.poolingTimeoutDuration && !d.stageContext.IsStopped() {
		fi := d.filesQueue.Pop()
		for fi != nil {
			if isFileEligible(fi.getFullPath(), fi.getModTime(), d.currentFileInfo, d.readOrder) {
				log.WithField("File Name", fi.getFullPath()).Debug("File picked for ingestion")
				d.setCurrentFileInfo(fi)
				return fi
			}
			fi = d.filesQueue.Pop()
		}
		log.Debugf("Sleeping for %d", d.spoolingPeriodDuration)
		time.Sleep(d.spoolingPeriodDuration)
	}
	return nil
}

func (d *DirectorySpooler) Destroy() {
	log.Info("Directory Spooler Destroy")
	if d.destroyNotificationChan != nil {
		d.destroyNotificationChan <- true
	}
}
