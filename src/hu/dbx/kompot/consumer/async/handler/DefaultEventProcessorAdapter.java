package hu.dbx.kompot.consumer.async.handler;

import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.EventDescriptorResolver;
import hu.dbx.kompot.consumer.async.EventStatusCallback;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.unmodifiableSet;

public final class DefaultEventProcessorAdapter implements EventProcessorAdapter, EventProcessorFactory, EventProcessorFactoryCollection, EventDescriptorResolver {

    private final Map<EventDescriptor, SelfDescribingEventProcessor> markerToProcessor = new ConcurrentHashMap<>();
    private final List<EventProcessorFactory> factories = new LinkedList<>();

    /**
     * Registers a new async event processor instance.
     */
    public void register(final SelfDescribingEventProcessor processor) {
        markerToProcessor.put(processor.getEventMarker(), processor);
    }

    @Override
    public void register(EventProcessorFactory factory) {
        factories.add(factory);
    }

    @Override
    public <TReq> void handle(EventDescriptor<TReq> eventMarker, TReq request, EventStatusCallback callback) {
        Optional<SelfDescribingEventProcessor<TReq>> processor = create(eventMarker);
        if (!processor.isPresent())
            throw new IllegalArgumentException("Can not handle event!");
        else
            processor.get().handle(request, callback);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <TReq> Optional<SelfDescribingEventProcessor<TReq>> create(EventDescriptor<TReq> eventMarker) {
        if (markerToProcessor.containsKey(eventMarker)) {
            return Optional.of(markerToProcessor.get(eventMarker));
        } else {
            return factories.stream().filter(f -> f.getSupportedEvents().contains(eventMarker)).findAny().flatMap(f -> f.create(eventMarker));
        }
    }

    @Override
    public Set<EventDescriptor> getSupportedEvents() {
        final Set<EventDescriptor> keys = new HashSet<>(markerToProcessor.keySet());
        factories.forEach(x -> keys.addAll(x.getSupportedEvents()));
        return unmodifiableSet(keys);
    }

    @Override
    public Optional<EventDescriptor> resolveMarker(String eventName) {
        if (null == eventName || eventName.isEmpty())
            throw new IllegalArgumentException("resolveMarker got empty eventName!");
        else
            return getSupportedEvents().stream().filter(x -> x.getEventName().equalsIgnoreCase(eventName)).findAny();
    }
}
