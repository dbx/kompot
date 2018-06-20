package hu.dbx.kompot.report;

import hu.dbx.kompot.core.KeyNaming;
import hu.dbx.kompot.impl.DataHandling;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static hu.dbx.kompot.impl.DataHandling.EventKeys.*;

/**
 * API for status reports
 */
@SuppressWarnings("unused")
public class Reporting implements EventQueries {

    private final KeyNaming keyNaming;
    private final JedisPool pool;

    public Reporting(KeyNaming naming, URI connection) {
        this.keyNaming = naming;
        this.pool = new JedisPool(connection);
    }

    public void resend(String uuid, String group) {
        // 1. TODO: make sure evt group is in failed state
        // 2. set event state to sending...
        // 3. put it back to queue

        // store.insertTimeItem(naming.eventCreationHistoryKey(), uuid + "," + group);
        // TODO: rremove from failure queue
    }

    /**
     * Returns sequence of all event group names that have had entries in db.
     */
    public Iterable<String> allEventGroups() {
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
        try (Jedis jedis = pool.getResource()) {

            final String methodKey = keyNaming.eventDetailsKey(uuid);

            return queryEvent(uuid, jedis);
        }
    }

    private Optional<EventData> queryEvent(UUID uuid, Jedis jedis) {

        final String eventDataKey = keyNaming.eventDetailsKey(uuid);

        final String name = jedis.hget(eventDataKey, CODE.name());
        if (name == null)
            return Optional.empty();

        final String data = jedis.hget(eventDataKey, DATA.name());
        final String groups = jedis.hget(eventDataKey, GROUPS.name());
        final String sender = jedis.hget(eventDataKey, SENDER.name());
        final String firstSent = jedis.hget(eventDataKey, FIRST_SENT.name());
        final String status = jedis.hget(eventDataKey, STATUS.name());
        //ERROR_MSG?

        EventData eventData = new EventData();

        eventData.setUuid(uuid);
        eventData.setData(data);
        eventData.setEventType(name);
        eventData.setGroups(groups);

        return Optional.of(eventData);
    }

    @Override
    public Collection<UUID> queryEventUuids(String group, EventFilters filters, Pagination pagination) {

        DataHandling.Statuses eventStatus = null;

        if (filters != null) {
            eventStatus = filters.getEventStatus();
        }

        final String s = keyNaming.unprocessedEventsByGroupKey(group);

        try (Jedis jedis = pool.getResource()) {

            final Set<String> uuids = new HashSet<>();

            if (eventStatus == null) {
                uuids.addAll(jedis.smembers(keyNaming.unprocessedEventsByGroupKey(group)));
                uuids.addAll(jedis.smembers(keyNaming.failedEventsByGroupKey(group)));
                uuids.addAll(jedis.smembers(keyNaming.processedEventsByGroupKey(group)));
            } else {
                switch (eventStatus) {
                    case ERROR:
                        uuids.addAll(jedis.smembers(keyNaming.unprocessedEventsByGroupKey(group)));
                        break;
                    case PROCESSED:
                        uuids.addAll(jedis.smembers(keyNaming.processedEventsByGroupKey(group)));
                        break;
                    case PROCESSING:
                        uuids.addAll(jedis.smembers(keyNaming.failedEventsByGroupKey(group)));
                        break;
                }
            }

            //TODO:pagination

            return uuids.stream().map(UUID::fromString).collect(Collectors.toList());

        }
    }
}
