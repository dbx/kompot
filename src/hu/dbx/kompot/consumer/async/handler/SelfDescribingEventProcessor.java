package hu.dbx.kompot.consumer.async.handler;

import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.EventStatusCallback;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface SelfDescribingEventProcessor<TReq> {

    EventDescriptor<TReq> getEventMarker();

    void handle(TReq request, EventStatusCallback callback);

    /**
     * Static constructor
     */
    static <TReq> SelfDescribingEventProcessor<TReq> of(EventDescriptor<TReq> event, BiConsumer<TReq, EventStatusCallback> bc) {
        return new SelfDescribingEventProcessor<TReq>() {
            @Override
            public EventDescriptor<TReq> getEventMarker() {
                return event;
            }

            @Override
            public void handle(TReq request, EventStatusCallback callback) {
                bc.accept(request, callback);
            }
        };
    }

    static <TReq> SelfDescribingEventProcessor<TReq> of(EventDescriptor<TReq> event, Consumer<TReq> handler) {
        return new SelfDescribingEventProcessor<TReq>() {

            @Override
            public EventDescriptor<TReq> getEventMarker() {
                return event;
            }

            @Override
            public void handle(TReq request, EventStatusCallback callback) {
                try {
                    handler.accept(request);
                    callback.success("OK");
                } catch (Throwable t) {
                    callback.error(t);
                }
            }
        };
    }
}
