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
	"syscall"
	"unsafe"
)

var (
	modkernel32        = syscall.NewLazyDLL("kernel32.dll")
	procLoadLibraryExW = modkernel32.NewProc("LoadLibraryExW")
)

func loadLibraryEx(libname string, loadMode uintptr) (handle syscall.Handle, err error) {
	var _p0 *uint16
	_p0, err = syscall.UTF16PtrFromString(libname)
	if err != nil {
		return
	}
	return _loadResourceLibrary(_p0, loadMode)
}

func _loadResourceLibrary(libname *uint16, loadMode uintptr) (handle syscall.Handle, err error) {
	r0, _, e1 := syscall.Syscall(procLoadLibraryExW.Addr(), 3, uintptr(unsafe.Pointer(libname)), 0, loadMode)
	handle = syscall.Handle(r0)
	if handle == 0 {
		if e1 != 0 {
			err = error(e1)
		} else {
			err = syscall.EINVAL
		}
	}
	return
}
