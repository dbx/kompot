package hu.dbx.kompot.consumer.async;

public interface EventSendingCallback {

    /**
     * Event has been sent
     */
    void onEventSent(EventFrame frame);

}
