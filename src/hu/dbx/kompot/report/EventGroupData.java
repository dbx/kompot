package hu.dbx.kompot.report;

import hu.dbx.kompot.impl.DataHandling;

public final class EventGroupData {

    private final EventData eventData;
    private final String eventGroupName;
    private final DataHandling.Statuses status;

    public EventGroupData(EventData eventData, String eventGroupName, DataHandling.Statuses status) {
        this.eventData = eventData;
        this.eventGroupName = eventGroupName;
        this.status = status;
    }

    public EventData getEventData() {
        return eventData;
    }

    public String getEventGroupName() {
        return eventGroupName;
    }

    public DataHandling.Statuses getStatus() {
        return status;
    }

    @Override
    public String toString() {
        return "Event Group Data of " + eventGroupName + ":" + status;
    }
}
