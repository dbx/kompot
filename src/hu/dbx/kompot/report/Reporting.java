package hu.dbx.kompot.report;

import hu.dbx.kompot.core.KeyNaming;
import hu.dbx.kompot.impl.DataHandling.Statuses;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static hu.dbx.kompot.impl.DataHandling.EventKeys.*;
import static java.util.stream.StreamSupport.stream;

/**
 * API for status reports
 */
public final class Reporting implements EventQueries {

    private final JedisPool pool;
    private final KeyNaming keyNaming;

    public Reporting(JedisPool pool, KeyNaming naming) {
        this.pool = pool;
        this.keyNaming = naming;
    }

    public void resend(String eventUuid, String eventGroup) {
        // 1. TODO: make sure evt eventGroup is in failed state
        // 2. set event state to sending...
        // 3. put it back to queue

        // store.insertTimeItem(naming.eventCreationHistoryKey(), eventUuid + "," + eventGroup);
        // TODO: rremove from failure queue
    }

    /**
     * Returns sequence of all event group names that have had entries in db.
     */
    public Iterable<String> listAllEventGroups() {
        //TODO: kell nekünk ilyen?
        throw new RuntimeException("Not implemented!");
    }

    @Override
    public ListResult<EventData> queryEvents(String group, EventFilters filters, Pagination pagination) {
        final ListResult<UUID> uuids = queryEventUuids(group, filters, pagination);

        try (Jedis jedis = pool.getResource()) {
            final List<EventData> data = stream(uuids.spliterator(), false)
                    .map(uuid -> queryEvent(jedis, uuid))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());
            return new ListResult<>(pagination.getOffset(), pagination.getLimit(), data.size(), data);
        }
    }

    @Override
    public Optional<EventData> querySingleEvent(UUID uuid) {
        if (uuid == null) {
            throw new IllegalArgumentException("Event uuid must not be null!");
        } else {
            try (Jedis jedis = pool.getResource()) {
                return queryEvent(jedis, uuid);
            }
        }
    }

    private Optional<EventData> queryEvent(Jedis jedis, UUID uuid) {
        final String eventDataKey = keyNaming.eventDetailsKey(uuid);
        final String eventType = jedis.hget(eventDataKey, CODE.name());

        if (eventType == null) {
            return Optional.empty();
        } else {
            final String data = jedis.hget(eventDataKey, DATA.name());
            final String groups = jedis.hget(eventDataKey, GROUPS.name());
            final String sender = jedis.hget(eventDataKey, SENDER.name());
            final LocalDateTime firstSent = LocalDateTime.parse(jedis.hget(eventDataKey, FIRST_SENT.name()));
            final Statuses eventStatus = Statuses.valueOf(jedis.hget(eventDataKey, STATUS.name()));
            //ERROR_MSG?

            final EventData eventData = new EventData(uuid, null, eventType, eventStatus, data, groups, sender, firstSent);

            return Optional.of(eventData);
        }
    }

    @Override
    public ListResult<UUID> queryEventUuids(String group, EventFilters filters, Pagination pagination) {
        if (group == null) {
            throw new IllegalArgumentException("Event group must not be null!");
        } else if (pagination == null) {
            throw new IllegalArgumentException("Pagination object must not be null!");
        }

        final Statuses eventStatus = filters != null ? filters.getEventStatus() : null;

        try (Jedis jedis = pool.getResource()) {
            final Set<String> eventUuids = new HashSet<>();

            if (eventStatus == null) {
                eventUuids.addAll(jedis.smembers(keyNaming.unprocessedEventsByGroupKey(group)));
                eventUuids.addAll(jedis.smembers(keyNaming.failedEventsByGroupKey(group)));
                eventUuids.addAll(jedis.smembers(keyNaming.processedEventsByGroupKey(group)));
            } else {
                switch (eventStatus) {
                    case ERROR:
                        eventUuids.addAll(jedis.smembers(keyNaming.unprocessedEventsByGroupKey(group)));
                        break;
                    case PROCESSED:
                        eventUuids.addAll(jedis.smembers(keyNaming.processedEventsByGroupKey(group)));
                        break;
                    case PROCESSING:
                        eventUuids.addAll(jedis.smembers(keyNaming.failedEventsByGroupKey(group)));
                        break;
                }
            }

            //TODO:pagination
            final List<UUID> uuids = eventUuids.stream().map(UUID::fromString).collect(Collectors.toList());
            return new ListResult<>(pagination.getOffset(), pagination.getLimit(), uuids.size(), uuids);
        }
    }
}
