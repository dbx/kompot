package hu.dbx.kompot.report;

import java.util.Optional;
import java.util.UUID;

public interface EventQueries {

    /**
     * Returns sequence of events for a given restriction.
     *
     * @param group      gruop name where events belong (not null!)
     * @param filters    maybe null filter object
     * @param pagination offset and page size (not null)
     * @return iterable of event items in an event group.
     * @throws IllegalArgumentException when required parameter is null
     */
    ListResult<EventData> queryEvents(String group, EventFilters filters, Pagination pagination);

    /**
     * Looks up a single event object by event uuid.
     *
     * @param uuid identifier of event
     * @return event data on empty
     * @throws IllegalArgumentException when uuid is null
     */
    Optional<EventData> querySingleEvent(UUID uuid) throws IllegalArgumentException;

    /**
     * Returns a sequence of event uuids for a given group with a filtering.
     *
     * @param group      group where events belong (not null)
     * @param filters    maybe null restrictions
     * @param pagination offset and page size object (not null)
     * @return iterable of event uuids.
     * @throws IllegalArgumentException when required parameter is null
     */
    ListResult<UUID> queryEventUuids(String group, EventFilters filters, Pagination pagination) throws IllegalArgumentException;
}
