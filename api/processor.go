package api

// Processor is sdc2go processor stage. Processor stages receive records from an origin
// or other processors stages, perform operations on the records and write them out so they can be
// processed by another processor or destination stages.
//
// Produce method - When running a pipeline, the sdc2go calls this method from the Processor stage with a
// batch of records to process.
// Parameter batch - the batch of records to process.
// Parameter batchMaker - records created by the Processor stage must be added to the BatchMaker
// for them to be available to the rest of the pipeline.
type Processor interface {
	Process(batch Batch, batchMaker BatchMaker) error
}
