package hu.dbx.kompot.consumer;

import hu.dbx.kompot.consumer.async.handler.EventProcessorAdapter;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastProcessorFactory;
import hu.dbx.kompot.consumer.sync.handler.DefaultMethodProcessorAdapter;
import hu.dbx.kompot.core.KeyNaming;

public interface Consumer {

    ConsumerIdentity getConsumerIdentity();

    KeyNaming getKeyNaming();

    EventProcessorAdapter getEventProcessorAdapter();

    // TODO: legyen ebbol interface!
    DefaultMethodProcessorAdapter getMethodProcessorAdapter();

    BroadcastProcessorFactory getBroadcastProcessorAdapter();
}
