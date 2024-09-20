package hu.dbx.kompot.consumer;

import hu.dbx.kompot.consumer.async.handler.EventProcessorAdapter;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastProcessorFactory;
import hu.dbx.kompot.consumer.sync.handler.DefaultMethodProcessorAdapter;

public interface Consumer {

    ConsumerIdentity getConsumerIdentity();

    EventProcessorAdapter getEventProcessorAdapter();

    // TODO: legyen ebbol interface!
    DefaultMethodProcessorAdapter getMethodProcessorAdapter();

    BroadcastProcessorFactory getBroadcastProcessorAdapter();
}
