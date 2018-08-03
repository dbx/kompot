package hu.dbx.kompot.consumer.async;

import java.util.Map;
import java.util.Optional;

/**
 * Used to find EventDescriptor when deserializing.
 */
public interface EventDescriptorResolver {

    /**
     * Tries to find EventDescriptor for a given eventName.
     * Returns empty when not found.
     *
     * @param eventName name of eventMarker to find
     * @return EventDescriptor with matching eventName.
     * @throws IllegalArgumentException when arg is null or empty
     */
    Optional<EventDescriptor> resolveMarker(String eventName);


    /**
     * Always resolves to a simple event.
     */
    EventDescriptorResolver DEFAULT = eventName -> Optional.of(EventDescriptor.of(eventName, Map.class));
}