package hu.dbx.kompot.consumer.broadcast.handler;

/**
 * Handles a broadcast message.
 */
public interface BroadcastProcessorAdapter {

    /**
     * Handles a broadcast message.
     *
     * @param descriptor      describes a broadcast type
     * @param broadcastedData broadcast data
     * @param <TReq>          type of broadcast payload
     */
    <TReq> void handle(BroadcastDescriptor<TReq> descriptor, TReq broadcastedData);
}
