package hu.dbx.kompot.consumer;

public interface Listener {

    MessageResult onMessage(String channel, Object message);

    void afterStarted();

    void afterStopped();

}
