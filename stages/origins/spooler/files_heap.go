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
	"container/heap"
	"strings"
	"sync"
)

type FilesHeap struct {
	fileInfos FileInfos
	readOrder string
}

func (h FilesHeap) Len() int {
	return len(h.fileInfos)
}

func (h FilesHeap) Less(i, j int) bool {
	return (h.readOrder == Timestamp &&
		(h.fileInfos[i].getModTime().Before(h.fileInfos[j].getModTime()) ||
			(h.fileInfos[i].getModTime().Equal(h.fileInfos[j].getModTime()) &&
				strings.Compare(h.fileInfos[i].getFullPath(), h.fileInfos[j].getFullPath()) < 0))) ||
		(h.readOrder == Lexicographical &&
			(strings.Compare(h.fileInfos[i].getFullPath(), h.fileInfos[j].getFullPath()) < 0 ||
				(strings.Compare(h.fileInfos[i].getFullPath(), h.fileInfos[j].getFullPath()) == 0 &&
					h.fileInfos[i].getModTime().Before(h.fileInfos[j].getModTime()))))
}

func (h FilesHeap) Swap(i, j int) {
	h.fileInfos[i], h.fileInfos[j] = h.fileInfos[j], h.fileInfos[i]
}

func (h *FilesHeap) Contains(path string) bool {
	for _, fInfo := range h.fileInfos {
		if fInfo.getFullPath() == path {
			return true
		}
	}
	return false
}

func (h *FilesHeap) Push(x interface{}) {
	h.fileInfos = append(h.fileInfos, x.(*AtomicFileInformation))
}

func (h *FilesHeap) Pop() interface{} {
	old := h.fileInfos
	n := len(old)
	if n > 0 {
		x := old[n-1]
		h.fileInfos = old[0 : n-1]
		return x
	}
	return nil
}

type SynchronizedFilesHeap struct {
	filesHeap *FilesHeap
	lock      *sync.RWMutex
}

func (sfh *SynchronizedFilesHeap) Push(atf *AtomicFileInformation) {
	sfh.lock.Lock()
	defer sfh.lock.Unlock()
	heap.Push(sfh.filesHeap, atf)
}

func (sfh *SynchronizedFilesHeap) Pop() *AtomicFileInformation {
	sfh.lock.Lock()
	defer sfh.lock.Unlock()
	if sfh.filesHeap.Len() > 0 {
		return heap.Pop(sfh.filesHeap).(*AtomicFileInformation)
	}
	return nil
}

func (sfh *SynchronizedFilesHeap) Contains(path string) bool {
	sfh.lock.Lock()
	defer sfh.lock.Unlock()
	return sfh.filesHeap.Contains(path)
}

func NewSynchronizedFilesHeap(readOrder string) *SynchronizedFilesHeap {
	filesQueue := &FilesHeap{fileInfos: FileInfos{}, readOrder: readOrder}
	heap.Init(filesQueue)
	return &SynchronizedFilesHeap{filesHeap: filesQueue, lock: &sync.RWMutex{}}
}
