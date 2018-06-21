package hu.dbx.kompot.report;

import hu.dbx.kompot.impl.DataHandling.Statuses;

public final class EventFilters {

    private final Statuses eventStatus;

    /**
     * Constructs a new filter instance for a given status
     *
     * @param status maybe null status
     * @return new filter instance
     */
    public static EventFilters forStatus(Statuses status) {
        return new EventFilters(status);
    }

    private EventFilters(Statuses eventStatus) {
        this.eventStatus = eventStatus;
    }

    public Statuses getEventStatus() {
        return eventStatus;
    }
}
