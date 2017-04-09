package runner

import "time"

type SourceOffsetTracker interface {
	/**
	 * Return if the source finished processing data.
	 *
	 * This is more of a historical method as it determines whether source is done reading by checking for special
	 * offset value. This method will only work (e.g. return true) for (Pull)Source - it will never return true
	 * for PushSource.
	 */
	IsFinished() bool

	SetOffset(newOffset string)

	/**
	 * Change offset for entity in the tracked offsets map and commit it to persistent store.
	 *
	 * @param entity Entity to be changed, null will disable changing the staged object (making this equivalent to commitOffsets() call)
	 * @param newOffset New offset for given entity, null will remove the entity from tracking map
	 */
	CommitOffset()

	/**
	 * Return currently staged offsets map.
	 *
	 * This method should return immutable version of the offsets map - thus changes to the returned map won't be
	 * reflected. Use methods on this interface to mutate the state.
	 */
	GetOffset() string

	/**
	 * Get time of lastly committed batch.
	 */
	GetLastBatchTime() time.Time
}
