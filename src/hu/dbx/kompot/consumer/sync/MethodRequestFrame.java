package hu.dbx.kompot.consumer.sync;

import hu.dbx.kompot.moby.MetaDataHolder;
import hu.dbx.kompot.producer.ProducerIdentity;

import java.util.UUID;

public final class MethodRequestFrame<Req> {

    /**
     * Object describing event type.
     */
    private final MethodDescriptor<Req, ?> methodMarker;

    /**
     * Event payload.
     */
    private final Req methodData;

    /**
     * Identifier of sender module.
     */
    private final String sourceIdentifier;

    /**
     * Meta data
     */
    private final MetaDataHolder metaData;


    /**
     * Unique identifier of method. Defaults to random UUID value.
     */
    private final UUID identifier;

    private MethodRequestFrame(UUID identifier, MethodDescriptor<Req, ?> methodMarker, Req methodData, String sourceIdentifier, MetaDataHolder metaData) {
        this.methodMarker = methodMarker;
        this.methodData = methodData;
        this.sourceIdentifier = sourceIdentifier;
        this.identifier = identifier;
        this.metaData = metaData;
    }

    @SuppressWarnings("unchecked")
    public static <Req> MethodRequestFrame<Req> build(UUID identifier, ProducerIdentity sender, MethodDescriptor<Req, ?> marker, Req request, MetaDataHolder metaData) {
        return new MethodRequestFrame(identifier, marker, request, sender.getIdentifier(), metaData);
    }

    @SuppressWarnings("unchecked")
    public static <Req> MethodRequestFrame<Req> build(ProducerIdentity sender, MethodDescriptor<Req, ?> marker, Req request, MetaDataHolder metaData) {
        return new MethodRequestFrame(UUID.randomUUID(), marker, request, sender.getIdentifier(), metaData);
    }

    public MethodDescriptor<Req, ?> getMethodMarker() {
        return methodMarker;
    }

    public Req getMethodData() {
        return methodData;
    }

    public String getSourceIdentifier() {
        return sourceIdentifier;
    }

    public UUID getIdentifier() {
        return identifier;
    }

    public MetaDataHolder getMetaData() {
        return metaData;
    }
}
