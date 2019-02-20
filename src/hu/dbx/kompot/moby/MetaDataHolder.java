package hu.dbx.kompot.moby;

import java.util.UUID;

public final class MetaDataHolder {

    public enum MetaDataFields {

        /**
         * Optional batch identifier for event.
         */
        BATCH_ID,

        /**
         * Identifier of context in which event was created.
         */
        CORRELATION_ID,

        /**
         * Optional feedback identifier.
         * The user generates this value before starting a long process. The user also needs to manually subscribe
         * to a feedback bus with the generated uuid. Then various stages of event processing can invoke feedback
         * events that are shown to subscribed users.
         */
        FEEDBACK_UUID,

        /**
         * Reference of user who initiated this event.
         */
        USER_REF,

    }

    private final String correlationId;
    private final String userRef;
    private final Long batchId;
    private final UUID feedbackUuid;

    public static MetaDataHolder fromCorrelationId(String correlationId) {
        return new MetaDataHolder(correlationId, null, null, null);
    }

    public static MetaDataHolder fromUserRef(String userRef) {
        return new MetaDataHolder(null, userRef, null, null);
    }

    public static MetaDataHolder build(String correlationId, String userRef, Long batchId) {
        return new MetaDataHolder(correlationId, userRef, batchId, null);
    }

    public MetaDataHolder withBatchId(Long newBatchId) {
        return new MetaDataHolder(correlationId, userRef, newBatchId, feedbackUuid);
    }

    public MetaDataHolder withFeedbackUuid(UUID newFeedbackUuid) {
        return new MetaDataHolder(correlationId, userRef, batchId, newFeedbackUuid);
    }

    /**
     * Returns a new copy with correlation id overridden.
     */
    public MetaDataHolder withCorrelationId(String newCorrId) {
        return new MetaDataHolder(newCorrId, userRef, batchId, feedbackUuid);
    }

    private MetaDataHolder(String correlationId, String userRef, Long batchId, UUID feedbackUuid) {
        this.correlationId = correlationId;
        this.userRef = userRef;
        this.batchId = batchId;
        this.feedbackUuid = feedbackUuid;
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

    public UUID getFeedbackUuid() {
        return feedbackUuid;
    }
}
