package api

type Origin interface {
	/**
	 * When running a pipeline, the Data Collector calls this method from the <code>Origin</code> stage to obtain a batch
	 * of records for processing.
	 * <p/>
	 * <code>Origin</code> stages should not block indefinitely within this method if there is no data. They should have
	 * an internal timeout after which they produce an empty batch. By doing so it gives the chance to other stages in
	 * pipeline to know that the pipeline is still healthy but there is no data coming; and potentially allowing
	 * notifications to external systems.
	 *
	 * @param lastSourceOffset the offset returned by the previous call to this method, or <code>NULL</code> if this
	 * method is being called for the first time ever.
	 * @param maxBatchSize the requested maximum batch size a single call to this method should produce.
	 * @param batchMaker records created by the <code>Source</code> stage must be added to the <code>BatchMaker</code>
	 * for them to be available to the rest of the pipeline.
	 * @return the offset for the next call to this method. If <code>NULL</code> is returned it means the
	 * <code>Source</code> stage has fully process that data, that no more data is to be expected and that the
	 * pipeline should finish once the current batch is fully processed.
	 * @throws StageException if the <code>Source</code> had an error while consuming data or creating records.
	 */
	Produce(lastSourceOffset string, maxBatchSize int, batchMaker BatchMaker) (string, error)
}
