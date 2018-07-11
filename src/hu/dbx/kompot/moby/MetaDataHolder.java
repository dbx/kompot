package hu.dbx.kompot.moby;

//TODO: ez egy nagyon moby specifikus megoldás, esetleg ki lehetne cserélni valami általánosabb implementációra
public class MetaDataHolder {

    public enum MetaDataFields {

        CORRELATION_ID,
        USER_REF

    }

    private final String correlationId;
    private final String userRef;

    public static MetaDataHolder fromCorrelationId(String correlationId) {
        return new MetaDataHolder(correlationId, null);
    }

    public static MetaDataHolder fromUserRef(String correlationId) {
        return new MetaDataHolder(correlationId, null);
    }

    public static MetaDataHolder build(String correlationId, String userRef) {
        return new MetaDataHolder(correlationId, userRef);
    }

    private MetaDataHolder(String correlationId, String userRef) {
        this.correlationId = correlationId;
        this.userRef = userRef;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getUserRef() {
        return userRef;
    }
}
