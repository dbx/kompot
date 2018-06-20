package hu.dbx.kompot.consumer.broadcast.handler;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.unmodifiableSet;

public final class DefaultBroadcastProcessorAdapter implements BroadcastProcessorAdapter, BroadcastProcessorFactory, BroadcastProcessorFactoryCollection, BroadcastDescriptorResolver {

    private final Map<BroadcastDescriptor, SelfDescribingBroadcastProcessor> markerToProcessor = new ConcurrentHashMap<>();
    private final List<BroadcastProcessorFactory> factories = new LinkedList<>();

    /**
     * Registers a new async event processor instance.
     */
    public void register(final SelfDescribingBroadcastProcessor processor) {
        markerToProcessor.put(processor.getBroadcastDescriptor(), processor);
    }

    @Override
    public void register(BroadcastProcessorFactory factory) {
        factories.add(factory);
    }

    @Override
    public <TReq> void handle(BroadcastDescriptor<TReq> eventMarker, TReq broadcastedData) {
        Optional<SelfDescribingBroadcastProcessor<TReq>> processor = create(eventMarker);
        if (!processor.isPresent())
            throw new IllegalArgumentException("Can not handle event!");
        else
            processor.get().handle(broadcastedData);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <TReq> Optional<SelfDescribingBroadcastProcessor<TReq>> create(BroadcastDescriptor<TReq> descriptor) {
        if (markerToProcessor.containsKey(descriptor)) {
            return Optional.of(markerToProcessor.get(descriptor));
        } else {
            return factories.stream().filter(f -> f.getSupportedBroadcasts().contains(descriptor)).findAny().flatMap(f -> f.create(descriptor));
        }
    }

    @Override
    public Set<BroadcastDescriptor> getSupportedBroadcasts() {
        final Set<BroadcastDescriptor> keys = new HashSet<>(markerToProcessor.keySet());
        factories.forEach(x -> keys.addAll(x.getSupportedBroadcasts()));
        return unmodifiableSet(keys);
    }

    @Override
    public Optional<BroadcastDescriptor> resolveMarker(String eventName) {
        if (eventName == null || eventName.isEmpty()) {
            throw new IllegalArgumentException("Event name must not be null or empty!");
        } else {
            return getSupportedBroadcasts().stream().filter(x -> x.getBroadcastCode().equalsIgnoreCase(eventName)).findAny();
        }
    }
}
