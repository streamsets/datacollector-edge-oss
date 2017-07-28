// +build 386 windows,amd64 windows

//Copied from https://github.com/streamsets/windataextractor/tree/master/dev/src/lib/win/eventlog
package windows

import (
	"bytes"
	"fmt"
	"log"
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
		log.Printf("ResourceLibraries - About to expand '%s' with %v", origValue, tokens)
		for k, v := range tokens {
			if strings.Contains(value, k) {
				value = strings.Replace(value, k, v, -1)
			}
		}
		log.Printf("ResourceLibraries - Expansion from='%s' to='%s'", origValue, value)
	}
	return value
}

func loadResourceLibrary(libname string) (handle syscall.Handle, err error) {
	log.Printf("[DEBUG] ResourceLibraries - Trying to load %s", libname)
	//win32: LOAD_LIBRARY_AS_IMAGE_RESOURCE | LOAD_LIBRARY_AS_DATAFILE
	var loadMode uintptr = 0x00000002 | 0x00000020
	handle, err = loadLibraryEx(libname, loadMode)
	log.Printf("[DEBUG] ResourceLibraries - Loaded %s %t", libname, handle != 0)
	return
}

func findLibNames(logName, appName string) (libs map[string]string, err error) {
	var key string
	if logName == "Application" {
		key = `SYSTEM\CurrentControlSet\Services\Eventlog\Application\` + appName
	} else if logName == "System" {
		key = `SYSTEM\CurrentControlSet\Services\Eventlog\System\` + appName
	} else if logName == "Security" {
		key = `SYSTEM\CurrentControlSet\Services\Eventlog\Security\` + appName
	}
	if entries, err := ReadFromRegistryKey(key, registryEntries); err == nil {
		return entries, nil
	} else {
		log.Printf("[WARN] ResourceLibraries - Application=%s could not find libraries under key=%s, error: %v", appName, key, err)
		return nil, err
	}
}

func getResourceLibraries(logName, appName string) map[string]syscall.Handle {
	librariesLock.RLock()
	if handles, found := appLibraries[appName]; found {
		librariesLock.RUnlock()
		log.Println("[DEBUG] ResourceLibraries - Application=%s libraries found (arleady in cache), %v", appName, handles)
		return handles
	} else {
		librariesLock.RUnlock()
		log.Printf("[DEBUG] ResourceLibraries - LogName=%s Application=%s looking for libraries", logName, appName)
		libNames, err := findLibNames(logName, appName)
		log.Printf("[DEBUG] ResourceLibraries - Application=%s uses libraries=%v", appName, libNames)
		handles = make(map[string]syscall.Handle)
		librariesLock.Lock()
		defer librariesLock.Unlock()
		if err == nil && len(libNames) > 0 {
			for libType, libName := range libNames {
				if len(libName) > 0 {
					libName = replaceEnvVars(libName)
					if handle, found := libraries[libName]; found {
						log.Printf("[DEBUG] ResourceLibraries - LibType=%s Application='%s' found library=%s handle in library cache",
							libType, appName, libName)
						handles[libType] = handle
					} else if handle, err := loadResourceLibrary(libName); err == nil {
						log.Printf("[DEBUG] ResourceLibraries - Application=%s loaded library=%s handle", appName, libName)
						libraries[libName] = handle
						handles[libType] = handle
					} else {
						log.Printf("[DEBUG] ResourceLibraries - Application=%s library=%s handle could not be loaded, error=%v "+
							"setting NIL", appName, libName, err)
						libraries[libName] = 0
						handles[libType] = 0
					}
				}
			}
		}
		appLibraries[appName] = handles
		log.Println("[DEBUG] ResourceLibraries - Application=%s libraries found (added to cache), %v", appName, handles)
		return handles
	}
}

func ReleaseResourceLibraries() {
	librariesLock.Lock()
	defer librariesLock.Unlock()
	log.Printf("[DEBUG] ResourceLibraries - Releasing cached resource libraries, count=%d", len(libraries))
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
	log.Printf("[DEBUG] ResourceLibraries - FormatMessage handle=%d id=%d err=%v", handle, id, err)
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

func findResourceString(logName string, libType string, event *EventLogRecord, resourceId uint32) string {
	handles := getResourceLibraries(logName, event.SourceName)
	if msgTemplate, err := getMessageString(handles[libType], resourceId); err == nil {
		log.Printf("[DEBUG] ResourceLibraries - Found %s resource string for '%s' for resource %d: %s", libType, event.SourceName, resourceId, msgTemplate)
		return msgTemplate
	}
	log.Printf("[DEBUG] ResourceLibraries - Did not find %s resource string for '%s' for resource %d", libType, event.SourceName, resourceId)
	return ""
}

func findEventMessageTemplate(logName string, event *EventLogRecord) string {
	return findResourceString(logName, "EventMessageFile", event, event.EventID)
}

func findEventCategory(logName string, event *EventLogRecord) string {
	return findResourceString(logName, "CategoryMessageFile", event, uint32(event.EventCategory))
}
