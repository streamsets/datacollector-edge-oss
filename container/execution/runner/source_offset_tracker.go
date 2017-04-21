package runner

import "time"

// Offset tracker is for committing and retrieving offsets in a store.
//
// IsFinished Return if the source finished processing data.
// This is more of a historical method as it determines whether source is done reading by checking for special
// offset value. This method will only work (e.g. return true) for (Pull)Source - it will never return true
// for PushSource.
//
// CommitOffset Change offset for entity in the tracked offset and commit it to persistent store.
//
// GetOffset Return currently staged offset.
//
// GetLastBatchTime Get time of lastly committed batch.
type SourceOffsetTracker interface {
	IsFinished() bool

	SetOffset(newOffset string)

	CommitOffset()

	GetOffset() string

	GetLastBatchTime() time.Time
}
