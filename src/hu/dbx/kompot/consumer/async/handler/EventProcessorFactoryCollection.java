package hu.dbx.kompot.consumer.async.handler;

public interface EventProcessorFactoryCollection {

    void register(EventProcessorFactory factory);
}
