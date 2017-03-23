package api

type Batch interface {
	/**
	 * Returns the initial offset of the current batch.
	 * <p/>
	 * This return value should be treated as an opaque value as it is source dependent.
	 *
	 * @return the initial offset of the current batch.
	 */
	GetSourceOffset() string

	/**
	 * Returns an iterator with all the records in the batch for the current stage.
	 * <p/>
	 * Every time this method is called it returns a new iterator with all records in the batch.
	 *
	 * @return an iterator with all the records in the batch for the current stage.
	 */
	GetRecords() []Record
}
