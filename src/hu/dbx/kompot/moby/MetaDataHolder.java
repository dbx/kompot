package hu.dbx.kompot.moby;

import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Set;
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

        /**
         * name of "thing" who initiated this event.
         */
        SOURCE_NAME,

        /**
         * User roles who initiated this event.
         */
        USER_ROLES,

    }

    private String correlationId;
    private String userRef;
    private String sourceName;
    private Long batchId;
    private UUID feedbackUuid;
    private Set<String> userRoles;

    public static MetaDataHolder fromCorrelationId(String correlationId) {
        return new MetaDataHolder(correlationId, null, null, null, null, null);
    }

    public static MetaDataHolder fromUserRef(String userRef) {
        return new MetaDataHolder(null, userRef, null, null, null, null);
    }

    public static MetaDataHolder fromSourceName(String sourceName) {
        return new MetaDataHolder(null, null, sourceName, null, null, null);
    }

    public static MetaDataHolder build(String correlationId, String userRef, String sourceName, Long batchId) {
        return new MetaDataHolder(correlationId, userRef, sourceName, batchId, null, null);
    }

    public MetaDataHolder withBatchId(Long newBatchId) {
        return new MetaDataHolder(correlationId, userRef, sourceName, newBatchId, feedbackUuid, userRoles);
    }

    public MetaDataHolder withFeedbackUuid(UUID newFeedbackUuid) {
        return new MetaDataHolder(correlationId, userRef, sourceName, batchId, newFeedbackUuid, userRoles);
    }

    public MetaDataHolder withUserRoles(Set<String> newUserRoles) {
        return new MetaDataHolder(correlationId, userRef, sourceName, batchId, feedbackUuid, newUserRoles);
    }

    /**
     * Returns a new copy with correlation id overridden.
     */
    public MetaDataHolder withCorrelationId(String newCorrId) {
        return new MetaDataHolder(newCorrId, userRef, sourceName, batchId, feedbackUuid, userRoles);
    }

    public MetaDataHolder() {
    }

    private MetaDataHolder(String correlationId, String userRef, String sourceName, Long batchId, UUID feedbackUuid, Set<String> userRoles) {
        this.correlationId = correlationId;
        this.userRef = userRef;
        this.sourceName = sourceName;
        this.batchId = batchId;
        this.feedbackUuid = feedbackUuid;
        this.userRoles = userRoles;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getUserRef() {
        return userRef;
    }

    public String getSourceName() {
        return sourceName;
    }

    public Long getBatchId() {
        return batchId;
    }

    public UUID getFeedbackUuid() {
        return feedbackUuid;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public void setUserRef(String userRef) {
        this.userRef = userRef;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public void setBatchId(Long batchId) {
        this.batchId = batchId;
    }

    public void setFeedbackUuid(UUID feedbackUuid) {
        this.feedbackUuid = feedbackUuid;
    }

    public Set<String> getUserRoles() {
        return userRoles;
    }

    public void setUserRoles(Set<String> userRoles) {
        this.userRoles = userRoles;
    }

    @Override
    public String toString() {
        return new org.apache.commons.lang3.builder.ToStringBuilder(this, ToStringStyle.NO_CLASS_NAME_STYLE)
                .append("correlationId", correlationId)
                .append("userRef", userRef)
                .append("sourceName", sourceName)
                .append("batchId", batchId)
                .append("feedbackUuid", feedbackUuid)
                .append("userRoles", userRoles)
                .toString();
    }
}
