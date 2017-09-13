package util

import (
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
