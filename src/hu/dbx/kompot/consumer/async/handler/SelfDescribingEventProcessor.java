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
        };
    }

    static <TReq> SelfDescribingEventProcessor<TReq> of(EventDescriptor<TReq> event, BiConsumer<TReq, MetaDataHolder> bc) {
        return new SelfDescribingEventProcessor<TReq>() {
            @Override
            public EventDescriptor<TReq> getEventMarker() {
                return event;
            }

            @Override
            public void handle(TReq request, MetaDataHolder meta, EventStatusCallback callback) {
                try {
                    bc.accept(request, meta);
                    callback.success("OK");
                } catch (Throwable t) {
                    callback.error(t);
                }
            }
        };
    }

    static <TReq> SelfDescribingEventProcessor<TReq> of(EventDescriptor<TReq> event, Consumer<TReq> handler) {
        return of(event, (eventData, metaData) -> handler.accept(eventData));
    }

//    static <TReq> SelfDescribingEventProcessor<TReq> of(EventDescriptor<TReq> event, BiConsumer<TReq, EventStatusCallback> bc) {
//        return new SelfDescribingEventProcessor<TReq>() {
//            @Override
//            public EventDescriptor<TReq> getEventMarker() {
//                return event;
//            }
//
//            @Override
//            public void handle(TReq request, MetaDataHolder meta, EventStatusCallback callback) {
//                bc.accept(request, callback);
//            }
//        };
//    }


    @FunctionalInterface
    interface TriConsumer<T, U, V> {
        void accept(T t, U u, V v);

//        default TriConsumer<T, U, V> andThen(TriConsumer<? super T, ? super U, ? super V> after) {
//            Objects.requireNonNull(after);
//            return (a, b, c) -> {
//                accept(a, b, c);
//                after.accept(a, b, c);
//            };
//        }
    }
}
