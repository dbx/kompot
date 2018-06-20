package hu.dbx.kompot.producer;

import hu.dbx.kompot.consumer.async.EventDescriptor;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Megmondja, hogy egy esemeny tipust mely modul csoportoknak kell elkuldeni.
 */
@FunctionalInterface
public interface EventGroupProvider {

    /**
     * Dispatches to itself.
     */
    @SuppressWarnings("unused")
    static EventGroupProvider identity() {
        return marker -> singletonList(marker.getEventName());
    }

    /**
     * Dispatches to none.
     */
    @SuppressWarnings("unused")
    static EventGroupProvider empty() {
        return marker -> emptyList();
    }

    /**
     * Finds the processor groups for a given event object.
     * <p>
     * Used when sending events.
     *
     * @param marker description of an event object
     * @return list of event group names (never null)
     */
    Iterable<String> findEventGroups(EventDescriptor marker);
}
