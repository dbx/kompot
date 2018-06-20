package hu.dbx.kompot.consumer.async.handler;

import hu.dbx.kompot.consumer.async.EventDescriptor;

import java.util.Optional;
import java.util.Set;

public interface EventProcessorFactory {

    <TReq> Optional<SelfDescribingEventProcessor<TReq>> create(EventDescriptor<TReq> eventMarker);

    Set<EventDescriptor> getSupportedEvents();
}
