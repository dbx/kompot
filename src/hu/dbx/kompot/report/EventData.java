package hu.dbx.kompot.report;

import hu.dbx.kompot.impl.DataHandling;

import java.time.LocalDateTime;
import java.util.UUID;

public final class EventData {

    private final UUID uuid;
    private final String eventTarget;
    private final String eventType;
    private final DataHandling.Statuses eventStatus;
    private final String data;
    private final String groups;
    private final String sender;
    private final LocalDateTime firstSent;

    public EventData(UUID uuid,
                     String eventTarget,
                     String eventType,
                     DataHandling.Statuses eventStatus,
                     String data,
                     String groups,
                     String sender,
                     LocalDateTime firstSent) {
        this.uuid = uuid;
        this.eventTarget = eventTarget;
        this.eventType = eventType;
        this.eventStatus = eventStatus;
        this.data = data;
        this.groups = groups;
        this.sender = sender;
        this.firstSent = firstSent;
    }

    public UUID getUuid() {
        return uuid;
    }

    public String getEventTarget() {
        return eventTarget;
    }

    public String getEventType() {
        return eventType;
    }

    public DataHandling.Statuses getEventStatus() {
        return eventStatus;
    }

    public String getData() {
        return data;
    }

    public String getGroups() {
        return groups;
    }

    public String getSender() {
        return sender;
    }

    public LocalDateTime getFirstSent() {
        return firstSent;
    }
}
