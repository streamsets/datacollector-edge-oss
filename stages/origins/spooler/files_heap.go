package spooler

import (
	"container/heap"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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
	fInfoStore atomic.Value
}

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
	fInfoStore := atomic.Value{}
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

type FilesHeap []*AtomicFileInformation

var readOrder string = LAST_MODIFIED //TODO

func (h FilesHeap) Len() int {
	return len(h)
}

func (h FilesHeap) Less(i, j int) bool {
	return (readOrder == LAST_MODIFIED &&
		(h[i].getModTime().Before(h[j].getModTime()) ||
			(h[i].getModTime().Equal(h[j].getModTime()) &&
				strings.Compare(h[i].getFullPath(), h[j].getFullPath()) < 0))) ||
		(readOrder == LEXICOGRAPHICAL &&
			(strings.Compare(h[i].getFullPath(), h[j].getFullPath()) < 0 ||
				(strings.Compare(h[i].getFullPath(), h[j].getFullPath()) == 0 &&
					h[i].getModTime().Before(h[j].getModTime()))))
}

func (h FilesHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *FilesHeap) Contains(path string) bool {
	for _, fInfo := range *h {
		if fInfo.getFullPath() == path {
			return true
		}
	}
	return false
}

func (h *FilesHeap) Push(x interface{}) {
	*h = append(*h, x.(*AtomicFileInformation))
}

func (h *FilesHeap) Pop() interface{} {
	old := *h
	n := len(old)
	if n > 0 {
		x := old[n-1]
		*h = old[0 : n-1]
		return x
	}
	return nil
}

type SynchronizedFilesHeap struct {
	filesQueue *FilesHeap
	lock       *sync.RWMutex
}

func (sfh *SynchronizedFilesHeap) Push(atf *AtomicFileInformation) {
	sfh.lock.Lock()
	defer sfh.lock.Unlock()
	heap.Push(sfh.filesQueue, atf)
}

func (sfh *SynchronizedFilesHeap) Pop() *AtomicFileInformation {
	sfh.lock.Lock()
	defer sfh.lock.Unlock()
	if len(*(sfh.filesQueue)) > 0 {
		return heap.Pop(sfh.filesQueue).(*AtomicFileInformation)
	}
	return nil
}

func (sfh *SynchronizedFilesHeap) Contains(path string) bool {
	sfh.lock.Lock()
	defer sfh.lock.Unlock()
	return sfh.filesQueue.Contains(path)
}

func NewSynchronizedFilesHeap() *SynchronizedFilesHeap {
	filesQueue := &FilesHeap{}
	heap.Init(filesQueue)
	return &SynchronizedFilesHeap{filesQueue: filesQueue, lock: &sync.RWMutex{}}
}
