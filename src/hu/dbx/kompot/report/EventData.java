package hu.dbx.kompot.report;

import hu.dbx.kompot.impl.DataHandling;

import java.util.UUID;

public class EventData {

    private UUID uuid;
    private String eventTarget;
    private String eventType;
    private DataHandling.Statuses eventStatus;
    private String data;
    private String groups;
    private String sender;
    private String firstSent;

    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    public String getEventTarget() {
        return eventTarget;
    }

    public void setEventTarget(String eventTarget) {
        this.eventTarget = eventTarget;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public DataHandling.Statuses getEventStatus() {
        return eventStatus;
    }

    public void setEventStatus(DataHandling.Statuses eventStatus) {
        this.eventStatus = eventStatus;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getGroups() {
        return groups;
    }

    public void setGroups(String groups) {
        this.groups = groups;
    }

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public String getFirstSent() {
        return firstSent;
    }

    public void setFirstSent(String firstSent) {
        this.firstSent = firstSent;
    }
}
