package hu.dbx.kompot.consumer.async;

public interface EventStatusCallback {

    /**
     * Note successful status.
     * Send null message to save space.
     *
     * @param message - optional message to administrator to denote status.
     * @throws IllegalStateException iff some other callback function has been called already.
     */
    void success(String message) throws IllegalStateException;

    /**
     * Marks successful status.
     *
     * @throws IllegalStateException iff some other callback function has beed called already.
     * @see #success(String)
     */
    default void success() {
        success(null);
    }

    /**
     * Note that an error has happened. Show message to administrator.
     *
     * @throws IllegalStateException iff some other callback function has been called already.
     */
    void error(String e) throws IllegalStateException;

    /**
     * Note that an error has happened due to an exception.
     *
     * @throws IllegalStateException iff some other callback function has been called already.
     */
    void error(Throwable e) throws IllegalStateException;

    void markProcessing();

    void setFrame(EventFrame frame);
}
