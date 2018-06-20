package hu.dbx.kompot.consumer.broadcast.handler;

import java.util.Optional;
import java.util.Set;

/**
 * Creates broadcast processors for broadcast types.
 */
public interface BroadcastProcessorFactory {

    /**
     * Returns a broadcast processor when found for the given broadcast descriptor.
     * @param descriptor
     * @param <TReq>
     * @return
     */
    <TReq> Optional<SelfDescribingBroadcastProcessor<TReq>> create(BroadcastDescriptor<TReq> descriptor);

    Set<BroadcastDescriptor> getSupportedBroadcasts();
}
