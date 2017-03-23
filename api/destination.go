package api

type Destination interface {
	/**
   	 * When running a pipeline, the Data Extractor calls this method from the <code>Target</code> stage to write a batch
   	 * of records to an external system.
   	 * <p/>
   	 * @param batch the batch of record to write to the external system.
   	 * @throws StageException if the <code>Target</code> had an error while writing to the external system.
   	 */
	Write(batch Batch) (error)
}