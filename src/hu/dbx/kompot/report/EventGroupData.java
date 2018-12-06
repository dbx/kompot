package hu.dbx.kompot.report;

import hu.dbx.kompot.impl.DataHandling;

import java.util.Collections;
import java.util.List;

public final class EventGroupData {

    private final EventData eventData;
    private final String eventGroupName;
    private final DataHandling.Statuses status;
    private final List<EventGroupHistoryItem> history;

    EventGroupData(EventData eventData, String eventGroupName, DataHandling.Statuses status, List<EventGroupHistoryItem> history) {
        this.eventData = eventData;
        this.eventGroupName = eventGroupName;
        this.status = status;
        this.history = history;
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

    /**
     * Returns an unmodifiable list of history items. Never empty.
     */
    public List<EventGroupHistoryItem> getHistory() {
        return Collections.unmodifiableList(history);
    }

    @Override
    public String toString() {
        return "Event Group Data of " + eventGroupName + ":" + status;
    }
}
