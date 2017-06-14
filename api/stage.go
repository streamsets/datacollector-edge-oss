package api

// Stage is the base interface for sdc2go stages implementations defining their common context and lifecycle.
//
// Init method initializes the stage.
// This method is called once when the pipeline is being initialized before the processing any data.
// If the stage returns an empty list of ConfigIssue then the stage is considered ready to process data.
// Else it is considered it is mis-configured or that there is a problem and the stage is not ready to process data,
// thus aborting the pipeline initialization.
//
// Destroy method destroys the stage. It should be used to release any resources held by the stage after initialization
// or processing.
// This method is called once when the pipeline is being shutdown. After this method is called, the stage will not
// be called to process any more data.
// This method is also called after a failed initialization to allow releasing resources created before the
// initialization failed.
type Stage interface {
	Init(stageContext StageContext) error
	Destroy() error
}
