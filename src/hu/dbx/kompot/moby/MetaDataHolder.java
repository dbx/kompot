package hu.dbx.kompot.moby;

public final class MetaDataHolder {

    public enum MetaDataFields {

        /**
         * Identifier of context in which event was created.
         */
        CORRELATION_ID,

        /**
         * Reference of user who initiated this event.
         */
        USER_REF,

        /**
         * Optional batch identifier for event.
         */
        BATCH_ID
    }

    private final String correlationId;
    private final String userRef;
    private final Long batchId;

    public static MetaDataHolder fromCorrelationId(String correlationId) {
        return new MetaDataHolder(correlationId, null, null);
    }

    public static MetaDataHolder fromUserRef(String correlationId) {
        return new MetaDataHolder(correlationId, null, null);
    }

    public static MetaDataHolder build(String correlationId, String userRef, Long batchId) {
        return new MetaDataHolder(correlationId, userRef, batchId);
    }

    private MetaDataHolder(String correlationId, String userRef, Long batchId) {
        this.correlationId = correlationId;
        this.userRef = userRef;
        this.batchId = batchId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getUserRef() {
        return userRef;
    }

    public Long getBatchId() {
        return batchId;
    }
}
