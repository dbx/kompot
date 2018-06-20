package hu.dbx.kompot.report;

import java.util.Optional;
import java.util.UUID;

public interface EventQueries {

    Iterable<EventData> queryEvents(String group, EventFilters filters, Pagination pagination);

    Optional<EventData> querySingleEvent(UUID uuid);

    Iterable<UUID> queryEventUuids(String group, EventFilters filters, Pagination pagination);
}
