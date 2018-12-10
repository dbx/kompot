package hu.dbx.kompot.consumer.broadcast.handler;

import hu.dbx.kompot.exceptions.SerializationException;

import java.util.function.Consumer;

public interface SelfDescribingBroadcastProcessor<TReq> {

    BroadcastDescriptor<TReq> getBroadcastDescriptor();

    void handle(TReq request) throws SerializationException;

    /**
     * Static constructor
     */
    static <TReq> SelfDescribingBroadcastProcessor<TReq> of(BroadcastDescriptor<TReq> event, Consumer<TReq> bc) {
        return new SelfDescribingBroadcastProcessor<TReq>() {
            @Override
            public BroadcastDescriptor<TReq> getBroadcastDescriptor() {
                return event;
            }

            @Override
            public void handle(TReq request) {
                bc.accept(request);
            }
        };
    }
}
