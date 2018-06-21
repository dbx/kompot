package hu.dbx.kompot.consumer.sync;

public interface MethodSendingEventListener {

    /**
     * Request has been sent now we are waiting for responses.
     */
    void onRequestSent(MethodRequestFrame frame);

    /**
     * Method processing was successful, received response.
     */
    void onResponseReceived(MethodRequestFrame requestFrame, Object response);

    /**
     * An error occurred during processing the method on the other side.
     */
    void onErrorReceived(MethodRequestFrame requestFrame, Throwable t);

    /**
     * We no longer wait on method, it timed out.
     * Maybe receiving process is not running?
     */
    void onTimeOut(MethodRequestFrame requestFrame);
}
