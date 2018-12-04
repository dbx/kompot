package hu.dbx.kompot.consumer.async;

import hu.dbx.kompot.consumer.async.handler.SelfDescribingEventProcessor;

public abstract class AbstractSelfDescribingEventProcessor<T> implements SelfDescribingEventProcessor<T> {

    private final EventDescriptor<T> eventMarker;

    @SuppressWarnings("WeakerAccess")
    public AbstractSelfDescribingEventProcessor(EventDescriptor<T> eventMarker) {
        this.eventMarker = eventMarker;
    }

    @Override
    public EventDescriptor<T> getEventMarker() {
        return eventMarker;
    }

    @Override
    public String toString() {
        return "<Event Decr" + getEventMarker().getEventName() + "." + getEventMarker().getPriority() + ">";
    }
}
