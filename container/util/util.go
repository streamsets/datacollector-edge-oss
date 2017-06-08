package util

import "time"

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
