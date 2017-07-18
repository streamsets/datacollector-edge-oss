package spooler

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type DirectorySpooler struct {
	currentFileInfo         *AtomicFileInformation
	dirPath                 string
	processSubDirs          bool
	maxNumberOfFiles        int
	destroyNotificationChan chan (bool)
	filesQueue              *SynchronizedFilesHeap
	spoolWaitDuration       time.Duration
	readOrder               string
	filePattern             string
	currentFileChange       chan (*AtomicFileInformation)
}

func isFileEligible(
	path string,
	modTime time.Time,
	currentFileInformation *AtomicFileInformation,
	readOrder string,
) bool {
	if currentFileInformation == nil {
		return true
	}
	return (readOrder == LAST_MODIFIED &&
		(modTime.After(currentFileInformation.getModTime()) ||
			(modTime.Equal(currentFileInformation.getModTime()) &&
				strings.Compare(path, currentFileInformation.getFullPath()) > 0))) ||
		(readOrder == LEXICOGRAPHICAL &&
			(strings.Compare(path, currentFileInformation.getFullPath()) > 0 ||
				(strings.Compare(path, currentFileInformation.getFullPath()) == 0 &&
					modTime.After(currentFileInformation.getModTime()))))
}

func (d *DirectorySpooler) addPathToQueueIfEligible(
	path string,
	modTime time.Time,
	currentFileInfo *AtomicFileInformation,
) {
	if isFileEligible(path, modTime, currentFileInfo, d.readOrder) {
		fileInfo := NewAtomicFileInformation(path, modTime, 0)
		if !d.filesQueue.Contains(fileInfo.getFullPath()) {
			log.Printf("[DEBUG] Pushing %s to queue", fileInfo.createOffset())
			d.filesQueue.Push(fileInfo)
		}
	} else {
		log.Printf("[DEBUG] File '%s' ignored because it is not eligible", path)
	}
}

func (d *DirectorySpooler) walkDirectoryPath(currentFileInfo *AtomicFileInformation) error {
	log.Println("[INFO] Spooler Starting")
	//TODO: Guard against links
	if d.processSubDirs {
		return filepath.Walk(d.dirPath, func(path string, info os.FileInfo, err error) error {
			if err == nil && path != d.dirPath {
				if !info.IsDir() {
					matched, err := filepath.Match(d.filePattern, info.Name())
					if err == nil && matched {
						d.addPathToQueueIfEligible(path, info.ModTime(), currentFileInfo)
					}
				}

			}
			return err
		})
	}
	file_matches, err := filepath.Glob(d.dirPath + "/" + d.filePattern)
	if err == nil {
		for _, file_match := range file_matches {
			file_info, err := os.Stat(file_match)
			if err != nil {
				return err
			}
			d.addPathToQueueIfEligible(file_match, file_info.ModTime(), currentFileInfo)
		}
	}
	return err
}

func (d *DirectorySpooler) Init() {
	d.destroyNotificationChan = make(chan (bool))
	d.filesQueue = NewSynchronizedFilesHeap()
	d.currentFileChange = make(chan (*AtomicFileInformation))
	//Starting Spooler immediately and after that at regular intervals
	d.walkDirectoryPath(d.currentFileInfo)
	go func(currentFileInfo *AtomicFileInformation) {
		end := false
		for !end {
			select {
			case <-time.After(d.spoolWaitDuration):
				d.walkDirectoryPath(currentFileInfo)
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
	fi := d.filesQueue.Pop()
	for fi != nil  {
		if isFileEligible(fi.getFullPath(), fi.getModTime(), d.currentFileInfo, d.readOrder) {
			log.Printf("[DEBUG] File '%s' is picked for ingestion", fi.getFullPath())
			d.setCurrentFileInfo(fi)
			return fi
		}
		fi = d.filesQueue.Pop()
	}
	return nil
}

func (d *DirectorySpooler) Destroy() {
	log.Println("Directory Spooler Destroy")
	d.destroyNotificationChan <- true
}
