package api

import "github.com/streamsets/dataextractor/container/common"

/**
 * Base interface for Data Extractor stages implementations defining their common context and lifecycle.
 *
 * @see Origin
 * @see Destination
 */
type Stage interface {

	/**
	 * Initializes the stage.
	 * <p/>
	 * This method is called once when the pipeline is being initialized before the processing any data.
	 * <p/>
	 * If the stage returns an empty list of {@link ConfigIssue}s then the stage is considered ready to process data.
	 * Else it is considered it is mis-configured or that there is a problem and the stage is not ready to process data,
	 * thus aborting the pipeline initialization.
	 *
	 * @param info the stage information.
	 * @param context the stage context.
	 */
	Init(stageConfig common.StageConfiguration)

	/**
	 * Destroys the stage. It should be used to release any resources held by the stage after initialization or
	 * processing.
	 * <p/>
	 * This method is called once when the pipeline is being shutdown. After this method is called, the stage will not
	 * be called to process any more data.
	 * <p/>
	 * This method is also called after a failed initialization to allow releasing resources created before the
	 * initialization failed.
	 */
	Destroy()
}
