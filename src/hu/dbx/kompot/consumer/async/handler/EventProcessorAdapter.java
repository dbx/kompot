package hu.dbx.kompot.consumer.async.handler;

import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.EventStatusCallback;

public interface EventProcessorAdapter {

    /**
     * Override this to handle an incoming event.
     */
    <TReq> void handle(EventDescriptor<TReq> eventMarker, TReq request, EventStatusCallback callback);
}