package hu.dbx.kompot.producer;

import java.util.UUID;

/**
 * Egyertelmuen meghataroz egy klienst. A kliens az esemenyek/uzenetek forrasa.
 */
public interface ProducerIdentity {

    static ProducerIdentity constantly(String s) {
        return () -> s;
    }

    @SuppressWarnings("unused")
    static ProducerIdentity randomUuidIdentity() {
        return constantly(UUID.randomUUID().toString());
    }

    /**
     * Unique identifier of the current producer instance.
     * <p>
     * May change during restarts. Must not change during runtime.
     * Might contain UUID string, program name, version, hash, etc.
     * <p>
     * Used for debugging purposes only.
     *
     * @return unique identifier string.
     */
    String getIdentifier();
}
