// +build 386 windows,amd64 windows

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

// Copied from https://github.com/streamsets/windataextractor/tree/master/dev/src/lib/win/eventlog
package eventlogging

import (
	"bytes"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

var (
	registryEntries = []string{"CategoryMessageFile", "EventMessageFile", "ParameterMessageFile"}

	libraries     = make(map[string]syscall.Handle)
	appLibraries  = make(map[string]map[string]syscall.Handle)
	librariesLock = sync.RWMutex{}
	env           = make(map[string]string)
)

func init() {
	for _, kv := range os.Environ() {
		kvp := strings.SplitN(kv, "=", 2)
		env[strings.ToUpper("%"+kvp[0]+"%")] = strings.ToUpper(kvp[1])
	}
}

func replaceEnvVars(value string) string {
	value = strings.ToUpper(value)
	return replaceTokens(value, "%", env)
}

func replaceTokens(value string, tokenSpecialChar string, tokens map[string]string) string {
	if strings.Contains(value, tokenSpecialChar) {
		origValue := value
		log.Infof("ResourceLibraries - About to expand '%s' with %v", origValue, tokens)
		for k, v := range tokens {
			if strings.Contains(value, k) {
				value = strings.Replace(value, k, v, -1)
			}
		}
		log.Infof("ResourceLibraries - Expansion from='%s' to='%s'", origValue, value)
	}
	return value
}

func loadResourceLibrary(libname string) (handle syscall.Handle, err error) {
	log.WithField("libname", libname).Debug("ResourceLibraries - Loading")
	//win32: LOAD_LIBRARY_AS_IMAGE_RESOURCE | LOAD_LIBRARY_AS_DATAFILE
	var loadMode uintptr = 0x00000002 | 0x00000020
	handle, err = loadLibraryEx(libname, loadMode)
	log.WithFields(log.Fields{"libname": libname, "loaded": handle != 0}).Debug("ResourceLibraries - Loaded")
	return
}

func findLibNames(logName, appName string) (libs map[string]string, err error) {
	key := `SYSTEM\CurrentControlSet\Services\Eventlog\` + logName + `\` + appName
	if entries, err := ReadFromRegistryKey(key, registryEntries); err == nil {
		return entries, nil
	} else {
		log.Warnf("ResourceLibraries - Application=%s could not find libraries under key=%s, error: %v", appName, key, err)
		return nil, err
	}
}

func getResourceLibraries(logName, appName string) map[string]syscall.Handle {
	librariesLock.RLock()
	if handles, found := appLibraries[appName]; found {
		librariesLock.RUnlock()
		log.Debugf("ResourceLibraries - Application=%s libraries found (already in cache), %v", appName, handles)
		return handles
	} else {
		librariesLock.RUnlock()
		log.Debugf("ResourceLibraries - LogName=%s Application=%s looking for libraries", logName, appName)
		libNames, err := findLibNames(logName, appName)
		log.Debugf("ResourceLibraries - Application=%s uses libraries=%v", appName, libNames)
		handles = make(map[string]syscall.Handle)
		librariesLock.Lock()
		defer librariesLock.Unlock()
		if err == nil && len(libNames) > 0 {
			for libType, libName := range libNames {
				if len(libName) > 0 {
					libName = replaceEnvVars(libName)
					if handle, found := libraries[libName]; found {
						log.Debugf("ResourceLibraries - LibType=%s Application='%s' found library=%s handle in library cache",
							libType, appName, libName)
						handles[libType] = handle
					} else if handle, err := loadResourceLibrary(libName); err == nil {
						log.Debugf("ResourceLibraries - Application=%s loaded library=%s handle", appName, libName)
						libraries[libName] = handle
						handles[libType] = handle
					} else {
						log.Debugf("ResourceLibraries - Application=%s library=%s handle could not be loaded, error=%v "+
							"setting NIL", appName, libName, err)
						libraries[libName] = 0
						handles[libType] = 0
					}
				}
			}
		}
		appLibraries[appName] = handles
		log.Debugf("ResourceLibraries - Application=%s libraries found (added to cache), %v", appName, handles)
		return handles
	}
}

func ReleaseResourceLibraries() {
	librariesLock.Lock()
	defer librariesLock.Unlock()
	log.WithField("count", len(libraries)).Debug("ResourceLibraries - Releasing cached resource libraries")
	for _, handle := range libraries {
		syscall.FreeLibrary(handle)
	}
	libraries = make(map[string]syscall.Handle)
	appLibraries = make(map[string]map[string]syscall.Handle)
}

func getMessageString(handle syscall.Handle, id uint32) (string, error) {
	var flags uint32 = syscall.FORMAT_MESSAGE_FROM_HMODULE | syscall.FORMAT_MESSAGE_FROM_SYSTEM |
		syscall.FORMAT_MESSAGE_IGNORE_INSERTS
	buffer := make([]uint16, 3000)
	//args are ignored because of syscall.FORMAT_MESSAGE_IGNORE_INSERTS in flag
	var dummyArgsRef *byte

	//using 0 as LANGID to trigger cascading lookup:
	//   http://msdn.microsoft.com/en-us/library/windows/desktop/ms679351%28v=vs.85%29.aspx
	n, err := syscall.FormatMessage(flags, uint32(handle), id, 0, buffer, dummyArgsRef)
	log.Debugf("ResourceLibraries - FormatMessage handle=%d id=%d err=%v", handle, id, err)
	if err != nil {
		return "", fmt.Errorf("Message template not found, handle=%d id=%d", handle, id)
	}
	// trim terminating \r and \n
	for ; n > 0 && (buffer[n-1] == '\n' || buffer[n-1] == '\r'); n-- {
	}
	msg := syscall.UTF16ToString(buffer[:n])
	return msg, nil
}

func messageF(value string, args []string) string {
	var msg string
	if len(value) > 0 {
		argMap := make(map[string]string)
		for idx, arg := range args {
			argMap["%"+strconv.Itoa(idx+1)] = arg
		}
		msg = replaceTokens(value, "%", argMap)
	} else {
		var buffer bytes.Buffer
		buffer.WriteString("NO_RES_MSG:")
		for _, arg := range args {
			buffer.WriteString(" ")
			buffer.WriteString(arg)
		}
		msg = buffer.String()
	}
	return msg
}

func findResourceString(logName string, libType string, event *eventLoggingRecord, resourceId uint32) string {
	handles := getResourceLibraries(logName, event.SourceName)
	if msgTemplate, err := getMessageString(handles[libType], resourceId); err == nil {
		log.Debugf("ResourceLibraries - Found %s resource string for '%s' for resource %d: %s", libType, event.SourceName, resourceId, msgTemplate)
		return msgTemplate
	}
	log.Debugf("ResourceLibraries - Did not find %s resource string for '%s' for resource %d", libType, event.SourceName, resourceId)
	return ""
}

func findEventMessageTemplate(logName string, event *eventLoggingRecord) string {
	return findResourceString(logName, "EventMessageFile", event, event.EventID)
}

func findEventCategory(logName string, event *eventLoggingRecord) string {
	return findResourceString(logName, "CategoryMessageFile", event, uint32(event.EventCategory))
}
