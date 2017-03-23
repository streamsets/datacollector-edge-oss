package api

type BatchMaker interface {
	/**
	 * Returns the available lane names (stream names) for the stage.
   	 *
   	 * @return the available lane names (stream names) for the stage.
   	 */
	GetLanes() []string

	/**
	 * Adds a record to the <code>BatchMaker</code>.
	 *
	 * @param record the record to add.
	 * to specify the lane name.
	 */
	AddRecord(record Record)
}
