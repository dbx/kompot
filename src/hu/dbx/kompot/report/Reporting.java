package hu.dbx.kompot.report;

import hu.dbx.kompot.core.KeyNaming;
import hu.dbx.kompot.impl.DataHandling.Statuses;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static hu.dbx.kompot.impl.DataHandling.EventKeys.*;

/**
 * API for status reports
 */
public final class Reporting implements EventQueries {

    private final KeyNaming keyNaming;
    private final JedisPool pool;

    public Reporting(KeyNaming naming, URI connection) {
        this.keyNaming = naming;
        this.pool = new JedisPool(connection);
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
    public Iterable<EventData> queryEvents(String group, EventFilters filters, Pagination pagination) {
        final Collection<UUID> uuids = queryEventUuids(group, filters, pagination);

        try (Jedis jedis = pool.getResource()) {

            return uuids.stream().map(uuid -> queryEvent(uuid, jedis)).filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
        }
    }

    @Override
    public Optional<EventData> querySingleEvent(UUID uuid) {
        if (uuid == null) {
            throw new IllegalArgumentException("Event uuid must not be null!");
        } else {
            try (Jedis jedis = pool.getResource()) {
                return queryEvent(uuid, jedis);
            }
        }
    }

    private Optional<EventData> queryEvent(UUID uuid, Jedis jedis) {
        final String eventDataKey = keyNaming.eventDetailsKey(uuid);
        final String eventType = jedis.hget(eventDataKey, CODE.name());

        if (eventType == null)
            return Optional.empty();

        final String data = jedis.hget(eventDataKey, DATA.name());
        final String groups = jedis.hget(eventDataKey, GROUPS.name());
        final String sender = jedis.hget(eventDataKey, SENDER.name());
        final LocalDateTime firstSent = LocalDateTime.parse(jedis.hget(eventDataKey, FIRST_SENT.name()));
        final Statuses eventStatus = Statuses.valueOf(jedis.hget(eventDataKey, STATUS.name()));
        //ERROR_MSG?

        final EventData eventData = new EventData(uuid, null, eventType, eventStatus, data, groups, sender, firstSent);

        return Optional.of(eventData);
    }

    @Override
    public Collection<UUID> queryEventUuids(String group, EventFilters filters, Pagination pagination) {
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
            return eventUuids.stream().map(UUID::fromString).collect(Collectors.toList());
        }
    }
}
