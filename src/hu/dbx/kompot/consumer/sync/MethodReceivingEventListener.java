package hu.dbx.kompot.consumer.sync;

/**
 * Used in Consumers to follow up on the lifecycle of processing an method.
 */
public interface MethodReceivingEventListener {

    /**
     * Request has been received, we are starting to process it.
     */
    void onRequestReceived(MethodRequestFrame frame);

    /**
     * Method processing was successful ready to send response.
     */
    void onRequestProcessedSuccessfully(MethodRequestFrame requestFrame, Object response);

    /**
     * An error occured durig the processing.
     */
    void onRequestProcessingFailure(MethodRequestFrame requestFrame, Throwable t);
}
