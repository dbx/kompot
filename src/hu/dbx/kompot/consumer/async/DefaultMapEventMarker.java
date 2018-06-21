package hu.dbx.kompot.consumer.async;

import java.util.Map;

/**
 * Helper to define simple events that deserialize to a java map.
 */
public final class DefaultMapEventMarker implements EventDescriptor<Map> {

    private final String eventName;

    public DefaultMapEventMarker(String eventName) {
        this.eventName = eventName;
    }

    @Override
    public String getEventName() {
        return eventName;
    }

    @Override
    public Class<? extends Map> getRequestClass() {
        return Map.class;
    }
}
