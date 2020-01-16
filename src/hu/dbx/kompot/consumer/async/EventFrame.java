package hu.dbx.kompot.consumer.async;

import hu.dbx.kompot.moby.MetaDataHolder;

import java.util.Collection;
import java.util.UUID;

/**
 * Describes a single event frame sent through the wire.
 * It is made a mutable object to support serialization.
 *
 * @param <Req> event payload type used for serializing.
 */
public class EventFrame<Req> {

    /**
     * Object describing event type.
     */
    private EventDescriptor<Req> eventMarker;

    /**
     * Event payload.
     */
    private Req eventData;

    /**
     * Meta data
     */
    private MetaDataHolder metaData;

    /**
     * Identifier of sender module.
     */
    private String sourceIdentifier;

    /**
     * Unique identifier of event. Defaults to random UUID value.
     */
    private UUID identifier = UUID.randomUUID();

    /**
     * The list of modules subscribed to this event
     */
    private Iterable<String> eventGroups;


    public static <Req> EventFrame<Req> build(EventDescriptor<Req> marker, Req request, MetaDataHolder metaData, Iterable<String> eventGroups) {
        final EventFrame<Req> eventFrame = new EventFrame<>();
        eventFrame.setEventMarker(marker);
        eventFrame.setEventData(request);
        eventFrame.setIdentifier(UUID.randomUUID());
        eventFrame.setMetaData(metaData);
        eventFrame.setEventGroups(eventGroups);
        return eventFrame;
    }

    public EventDescriptor getEventMarker() {
        return eventMarker;
    }

    public void setEventMarker(EventDescriptor<Req> eventMarker) {
        this.eventMarker = eventMarker;
    }

    public Req getEventData() {
        return eventData;
    }

    public void setEventData(Req eventData) {
        this.eventData = eventData;
    }

    public String getSourceIdentifier() {
        return sourceIdentifier;
    }

    public void setSourceIdentifier(String sourceIdentifier) {
        this.sourceIdentifier = sourceIdentifier;
    }

    public UUID getIdentifier() {
        return identifier;
    }

    public void setIdentifier(UUID identifier) {
        this.identifier = identifier;
    }

    public MetaDataHolder getMetaData() {
        return metaData;
    }

    public void setMetaData(MetaDataHolder metaData) {
        this.metaData = metaData;
    }

    public Iterable<String> getEventGroups() {
        return eventGroups;
    }

    public void setEventGroups(Iterable<String> eventGroups) {
        this.eventGroups = eventGroups;
    }

    @Override
    public String toString() {
        return "EventFrame{" +
                "eventMarker=" + eventMarker +
                ", eventData=" + eventData +
                ", sourceIdentifier='" + sourceIdentifier + '\'' +
                ", identifier='" + identifier + '\'' +
                '}';
    }

    public String debugSignature() {
        return getEventMarker().getEventName() + "@" + getIdentifier();
    }
}
