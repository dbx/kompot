package hu.dbx.kompot.consumer.broadcast.handler;

public interface BroadcastProcessorFactoryCollection {

    void register(BroadcastProcessorFactory factory);
}
