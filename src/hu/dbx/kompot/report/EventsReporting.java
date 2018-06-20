package hu.dbx.kompot.report;

import hu.dbx.kompot.consumer.async.EventFrame;

/**
 * Provides debug information about past events.
 * Does not mutate database.
 */
public class EventsReporting {
    // reports

//    private final ReadOnlyKVStoreFacade kvStoreFacade;

    //  public EventsReporting(ReadOnlyKVStoreFacade kvStoreFacade) {
    //      this.kvStoreFacade =kvStoreFacade;
//}

    // TODO: get a list of new/processing/processed/failed messages
    public ListResult<EventFrame> listNewEvents(Pagination p) {
        return null;
    }

    public ListResult<EventFrame> listProcessingEvents(Pagination p) {
        return null;
    }

    public ListResult<EventFrame> listProcessedEvents(Pagination p) {
        return null;
    }

    public ListResult<EventFrame> listFailedEvents(Pagination p) {
        return null;
    }

    // TODO: with pagination maybe???
    // TODO: make a different class for moving/resending messages.

}