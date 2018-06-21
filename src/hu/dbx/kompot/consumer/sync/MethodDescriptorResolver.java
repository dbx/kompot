package hu.dbx.kompot.consumer.sync;

import java.util.Optional;

/**
 * Used to find EventDescriptor when deserializing.
 */
public interface MethodDescriptorResolver {

    /**
     * Tries to find MethodDescriptor for a given methodName. Returns null when not found.
     *
     * @param methodName name of eventMarker to find
     * @return MethodDescriptor with same methodName or null
     * @throws IllegalArgumentException when arg is null or empty
     */
    Optional<MethodDescriptor> resolveMarker(String methodName);
}