package hu.dbx.kompot.consumer.async;

public interface EventReceivingCallback {

    /**
     * Event received, we will start processing it.
     */
    void onEventReceived(EventFrame eventFrame);

    /**
     * Successfully processed event
     */
    void onEventProcessedSuccessfully(EventFrame eventFrame, String message);

    /**
     * Failure during event processing
     */
    void onEventProcessingFailure(EventFrame eventFrame, Throwable t);

}
