package hu.dbx.kompot.consumer.async.handler;

import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.EventStatusCallback;
import hu.dbx.kompot.moby.MetaDataHolder;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface SelfDescribingEventProcessor<TReq> {

    EventDescriptor<TReq> getEventMarker();

    void handle(TReq request, MetaDataHolder meta, EventStatusCallback callback);

    /**
     * Static constructor
     */
    static <TReq> SelfDescribingEventProcessor<TReq> of(EventDescriptor<TReq> event, TriConsumer<TReq, MetaDataHolder, EventStatusCallback> bc) {
        return new SelfDescribingEventProcessor<TReq>() {
            @Override
            public EventDescriptor<TReq> getEventMarker() {
                return event;
            }

            @Override
            public void handle(TReq request, MetaDataHolder meta, EventStatusCallback callback) {
                bc.accept(request, meta, callback);
            }

            @Override
            public String toString() {
                return "<EventDescriptor of " + event.getEventName() + "." + event.getPriority() + ">";
            }
        };
    }

    static <TReq> SelfDescribingEventProcessor<TReq> of(EventDescriptor<TReq> event, BiConsumer<TReq, MetaDataHolder> bc) {
        return of(event, (a, b, c) -> {
            try {
                bc.accept(a, b);
                c.success("OK");
            } catch (Throwable t) {
                c.error(t.getMessage());
            }
        });
    }

    static <TReq> SelfDescribingEventProcessor<TReq> of(EventDescriptor<TReq> event, Consumer<TReq> handler) {
        return of(event, (eventData, metaData) -> handler.accept(eventData));
    }


    @FunctionalInterface
    interface TriConsumer<T, U, V> {
        void accept(T t, U u, V v);
    }
}
