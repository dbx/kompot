package hu.dbx.kompot.impl.consumer;

import hu.dbx.kompot.consumer.async.EventDescriptorResolver;
import hu.dbx.kompot.consumer.async.handler.EventProcessorAdapter;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptorResolver;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastProcessorFactory;
import hu.dbx.kompot.consumer.sync.MethodDescriptorResolver;
import hu.dbx.kompot.consumer.sync.handler.DefaultMethodProcessorAdapter;

/**
 * Immutable collection of Event/Method/Broadcast handlers for a consumer.
 */
public final class ConsumerHandlers {

    private final EventProcessorAdapter eventProcessorAdapter;
    private final EventDescriptorResolver eventResolver;

    private final BroadcastProcessorFactory broadcastProcessorFactory;
    private final BroadcastDescriptorResolver broadcastDescriptorResolver;

    private final DefaultMethodProcessorAdapter methodProcessorAdapter;
    private final MethodDescriptorResolver methodDescriptorResolver;

    public ConsumerHandlers(EventProcessorAdapter eventProcessorAdapter, EventDescriptorResolver eventResolver, BroadcastProcessorFactory broadcastProcessorFactory, BroadcastDescriptorResolver broadcastDescriptorResolver, DefaultMethodProcessorAdapter methodProcessorAdapter, MethodDescriptorResolver methodDescriptorResolver) {
        this.eventProcessorAdapter = eventProcessorAdapter;
        this.eventResolver = eventResolver;
        this.broadcastProcessorFactory = broadcastProcessorFactory;
        this.broadcastDescriptorResolver = broadcastDescriptorResolver;
        this.methodProcessorAdapter = methodProcessorAdapter;
        this.methodDescriptorResolver = methodDescriptorResolver;
    }

    public EventProcessorAdapter getEventProcessorAdapter() {
        return eventProcessorAdapter;
    }

    public EventDescriptorResolver getEventResolver() {
        return eventResolver;
    }

    public BroadcastProcessorFactory getBroadcastProcessorFactory() {
        return broadcastProcessorFactory;
    }

    public BroadcastDescriptorResolver getBroadcastDescriptorResolver() {
        return broadcastDescriptorResolver;
    }

    public DefaultMethodProcessorAdapter getMethodProcessorAdapter() {
        return methodProcessorAdapter;
    }

    public MethodDescriptorResolver getMethodDescriptorResolver() {
        return methodDescriptorResolver;
    }
}
