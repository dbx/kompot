package hu.dbx.kompot.report;

import java.time.LocalDateTime;
import java.util.UUID;

public final class EventData {

    private final UUID uuid;
    private final String eventType;
    private final String data;
    private final String groups;
    private final String sender;
    private final LocalDateTime firstSent;

    EventData(UUID uuid,
              String eventType,
              String data,
              String groups,
              String sender,
              LocalDateTime firstSent) {
        this.uuid = uuid;
        this.eventType = eventType;
        this.data = data;
        this.groups = groups;
        this.sender = sender;
        this.firstSent = firstSent;
    }

    public UUID getUuid() {
        return uuid;
    }

    public String getEventType() {
        return eventType;
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
