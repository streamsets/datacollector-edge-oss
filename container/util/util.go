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
package util

import (
	"github.com/sirupsen/logrus"
	"math/rand"
	"os"
	"strings"
	"time"
	"unicode"
)

func Contains(slice []string, e string) bool {
	for _, a := range slice {
		if a == e {
			return true
		}
	}
	return false
}

func ConvertTimeToLong(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond) / int64(time.Nanosecond)
}

func ConvertNanoToSecondsInt(nano int64) int64 {
	return nano / int64(time.Second) / int64(time.Nanosecond)
}

func ConvertNanoToSecondsFloat(nano float64) float64 {
	return nano / float64(time.Second) / float64(time.Nanosecond)
}

func UcFirst(str string) string {
	for i, v := range str {
		return string(unicode.ToUpper(v)) + str[i+1:]
	}
	return ""
}

func LcFirst(str string) string {
	for i, v := range str {
		return string(unicode.ToLower(v)) + str[i+1:]
	}
	return ""
}

func IsStringEmpty(str *string) bool {
	return str != nil && *str != ""
}

func CastToFloat64(value interface{}) interface{} {
	if value != nil {
		switch value.(type) {
		case uint8:
			return float64(value.(uint8))
		case uint16:
			return float64(value.(uint16))
		case uint32:
			return float64(value.(uint32))
		case uint64:
			return float64(value.(uint64))
		case int8:
			return float64(value.(int8))
		case int16:
			return float64(value.(int16))
		case int32:
			return float64(value.(int32))
		case int64:
			return float64(value.(int64))
		case int:
			return float64(value.(int))
		case float32:
			return float64(value.(float32))
		}
	}
	return value
}

func GetLastFieldNameFromPath(path string) string {
	pathArr := strings.Split(path, "/")
	return pathArr[len(pathArr)-1]
}

func CloseFile(file *os.File) {
	if err := file.Close(); err != nil {
		logrus.WithError(err).WithField("file", file.Name()).Error("Error During file close")
	}
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func IndexOf(word string, data []string) int {
	for k, v := range data {
		if word == v {
			return k
		}
	}
	return -1
}

func TruncateString(str string, num int) string {
	s := str
	if len(str) > num {
		s = str[0:num]
	}
	return s
}
