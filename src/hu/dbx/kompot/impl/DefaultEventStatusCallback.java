package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.async.EventFrame;
import hu.dbx.kompot.consumer.async.EventReceivingCallback;
import hu.dbx.kompot.consumer.async.EventStatusCallback;
import org.slf4j.Logger;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static hu.dbx.kompot.impl.LoggerUtils.debugEventFrame;

public class DefaultEventStatusCallback implements EventStatusCallback {

    private final UUID eventId;
    private final AtomicBoolean hasBeenCalled = new AtomicBoolean(false);
    private final AtomicReference<EventFrame> frame = new AtomicReference<>();
    private final List<EventReceivingCallback> eventReceivingCallbacks;

    private static final Logger LOGGER = LoggerUtils.getLogger();

    public DefaultEventStatusCallback(UUID eventId, List<EventReceivingCallback> eventReceivingCallbacks) {
        this.eventId = eventId;
        this.eventReceivingCallbacks = eventReceivingCallbacks;
    }

    @Override
    public void success(String message) throws IllegalStateException {
        if (hasBeenCalled.getAndSet(true))
            throw new IllegalStateException("Callback has been called once!");

        // success callbacks
        for (EventReceivingCallback eventReceivingCallback : eventReceivingCallbacks) {
            try {
                eventReceivingCallback.onEventProcessedSuccessfully(frame.get(), message);
            } catch (Throwable t) {
                debugEventFrame(LOGGER, frame.get());
                LOGGER.error("Error executing callback on event uuid=" + eventId, t);
            }
        }
    }

    @Override
    public void error(String e) throws IllegalStateException {
        error(new RuntimeException(e));
    }

    @Override
    public void error(Throwable e) throws IllegalStateException {
        if (hasBeenCalled.getAndSet(true))
            throw new IllegalStateException("Callback has been called once!");

        // failure callbacks
        for (EventReceivingCallback eventReceivingCallback : eventReceivingCallbacks) {
            try {
                eventReceivingCallback.onEventProcessingFailure(frame.get(), e);
            } catch (Throwable t) {
                debugEventFrame(LOGGER, frame.get());
                LOGGER.error("Error executing callback on event uuid=" + eventId, t);
            }
        }
    }

    @Override
    public void markProcessing() {

    }

    @Override
    public void setFrame(EventFrame frame) {
        assert frame != null;
        this.frame.set(frame);
    }

}
