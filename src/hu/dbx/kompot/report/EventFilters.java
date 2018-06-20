package hu.dbx.kompot.report;

import hu.dbx.kompot.impl.DataHandling;

public class EventFilters {

//    //az esemény cél feldolgozója (modulja)
//    private String target;

    //az esemény állapota
    private DataHandling.Statuses eventStatus;

    public DataHandling.Statuses getEventStatus() {
        return eventStatus;
    }

    public void setEventStatus(DataHandling.Statuses eventStatus) {
        this.eventStatus = eventStatus;
    }
}
