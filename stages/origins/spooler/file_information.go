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
	fInfoStore atomic.Value
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
