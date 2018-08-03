package hu.dbx.kompot.consumer.broadcast.handler;

import java.util.Optional;

/**
 * Used to find EventDescriptor when deserializing.
 */
public interface BroadcastDescriptorResolver {

    /**
     * Tries to find BroadcastDescriptor for a given eventName.
     * Returns empty when not found.
     *
     * @param eventName name of eventMarker to find
     * @return Broadcast item descriptor with matching brodacast code.
     * @throws IllegalArgumentException when arg is null or empty
     */
    Optional<BroadcastDescriptor> resolveMarker(String eventName);
}
