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
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"
)

type FileInformation struct {
	name         string
	directory    string
	modTime      time.Time
	offsetToRead int64
}

type AtomicFileInformation struct {
	fInfoStore  *atomic.Value
	customDelim string
}

type FileInfos []*AtomicFileInformation

func (atf *AtomicFileInformation) get() *FileInformation {
	fileInfo := atf.fInfoStore.Load().(FileInformation)
	return &fileInfo
}
func (atf *AtomicFileInformation) set(f *FileInformation) {
	atf.fInfoStore.Store(*f)
}

func NewAtomicFileInformation(path string, modTime time.Time, offsetToRead int64) *AtomicFileInformation {
	fileInfo := FileInformation{
		name:         filepath.Base(path),
		directory:    filepath.Dir(path),
		modTime:      modTime,
		offsetToRead: offsetToRead,
	}
	fInfoStore := &atomic.Value{}
	fInfoStore.Store(fileInfo)
	return &AtomicFileInformation{fInfoStore: fInfoStore}
}

func (atf *AtomicFileInformation) incOffsetToRead(offsetInc int64) {
	fInfo := atf.get()
	fInfo.offsetToRead += offsetInc
	atf.set(fInfo)
}

func (atf *AtomicFileInformation) setOffsetToRead(offset int64) {
	fInfo := atf.get()
	fInfo.offsetToRead = offset
	atf.set(fInfo)
}

func (atf *AtomicFileInformation) getOffsetToRead() int64 {
	return atf.get().offsetToRead
}

func (atf *AtomicFileInformation) getFullPath() string {
	f := atf.get()
	return filepath.Join(f.directory, f.name)
}

func (atf *AtomicFileInformation) getModTime() time.Time {
	return atf.get().modTime
}

func (atf *AtomicFileInformation) getName() string {
	return atf.get().name
}

func (atf *AtomicFileInformation) getDirectory() string {
	return atf.get().directory
}

func (atf *AtomicFileInformation) createOffset() string {
	f := atf.get()
	return atf.getFullPath() + "::" +
		strconv.FormatInt(atf.getOffsetToRead(), 10) + "::" +
		strconv.FormatInt(f.modTime.UnixNano(), 10)
}
