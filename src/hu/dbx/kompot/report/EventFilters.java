package hu.dbx.kompot.report;

import hu.dbx.kompot.impl.DataHandling;

public final class EventFilters {

    private final DataHandling.Statuses eventStatus;

    public EventFilters(DataHandling.Statuses eventStatus) {
        this.eventStatus = eventStatus;
    }

    public DataHandling.Statuses getEventStatus() {
        return eventStatus;
    }
}
