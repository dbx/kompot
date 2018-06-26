package hu.dbx.kompot.report;

import hu.dbx.kompot.core.KeyNaming;
import hu.dbx.kompot.impl.DataHandling;
import hu.dbx.kompot.impl.DataHandling.Statuses;
import hu.dbx.kompot.impl.LoggerUtils;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static hu.dbx.kompot.impl.DataHandling.EventKeys.*;
import static java.util.stream.StreamSupport.stream;

/**
 * API for status reports
 */
public final class Reporting implements EventQueries, EventUpdates {

    private static final Logger LOGGER = LoggerUtils.getLogger();

    private final JedisPool pool;
    private final KeyNaming keyNaming;

    public static Reporting ofRedisConnectionUri(URI connection, KeyNaming kn) {
        final JedisPool p = new JedisPool(connection);
        return new Reporting(p, kn);
    }

    public Reporting(JedisPool pool, KeyNaming naming) {
        this.pool = pool;
        this.keyNaming = naming;
    }

    @Override
    public Collection<String> listAllEventGroups() {
        try (Jedis jedis = pool.getResource()) {
            return jedis.smembers(keyNaming.eventGroupsKey());
        }
    }

    @Override
    public ListResult<EventGroupData> queryEvents(String group, EventFilters filters, Pagination pagination) {
        final ListResult<UUID> uuids = queryEventUuids(group, filters, pagination);

        try (Jedis jedis = pool.getResource()) {
            final List<EventGroupData> data = stream(uuids.spliterator(), false)
                    .map(uuid -> queryEventGroup(jedis, group, uuid))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());
            return new ListResult<>(pagination.getOffset(), pagination.getLimit(), data.size(), data);
        }
    }

    @Override
    public Optional<EventGroupData> querySingleEvent(String group, UUID uuid) {
        if (uuid == null) {
            throw new IllegalArgumentException("Event uuid must not be null!");
        } else {
            try (Jedis jedis = pool.getResource()) {
                return queryEventGroup(jedis, group, uuid);
            }
        }
    }

    private Optional<EventGroupData> queryEventGroup(Jedis jedis, String eventGroupName, UUID uuid) {

        Optional<EventData> eventData = queryEvent(jedis, uuid);

        if (!eventData.isPresent()) {
            LOGGER.error("Event data with uuid {} could not be loaded", uuid);
            return Optional.empty();
        }

        final String groupEventDataKey = keyNaming.eventDetailsKey(eventGroupName, uuid);
        final String groupEventStatusStr = jedis.hget(groupEventDataKey, STATUS.name());

        if (groupEventStatusStr == null) {
            LOGGER.error("Group event status with group {} and uuid {} could not be found", eventGroupName, uuid);
            return Optional.empty();
        }

        final Statuses groupEventStatus = Statuses.valueOf(groupEventStatusStr);

        EventGroupData eventGroupData = new EventGroupData(eventData.get(), eventGroupName, groupEventStatus);

        return Optional.of(eventGroupData);
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
            final String firstSentStr = jedis.hget(eventDataKey, FIRST_SENT.name());
            //ERROR_MSG?

            final LocalDateTime firstSent = LocalDateTime.parse(firstSentStr);
            final EventData eventData = new EventData(uuid, eventType, data, groups, sender, firstSent);

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

            final int offset = pagination.getOffset();
            final int limit = pagination.getLimit();

            if (eventStatus == null) {
                eventUuids.addAll(jedis.zrangeByScore(keyNaming.unprocessedEventsByGroupKey(group), Double.MIN_VALUE, Double.MAX_VALUE, offset, limit));
                eventUuids.addAll(jedis.zrangeByScore(keyNaming.processingEventsByGroupKey(group), Double.MIN_VALUE, Double.MAX_VALUE, offset, limit));
                eventUuids.addAll(jedis.zrangeByScore(keyNaming.processedEventsByGroupKey(group), Double.MIN_VALUE, Double.MAX_VALUE, offset, limit));
                eventUuids.addAll(jedis.zrangeByScore(keyNaming.failedEventsByGroupKey(group), Double.MIN_VALUE, Double.MAX_VALUE, offset, limit));
            } else {
                switch (eventStatus) {
                    case CREATED:
                        eventUuids.addAll(jedis.zrangeByScore(keyNaming.unprocessedEventsByGroupKey(group), Double.MIN_VALUE, Double.MAX_VALUE, offset, limit));
                        break;
                    case PROCESSING:
                        eventUuids.addAll(jedis.zrangeByScore(keyNaming.processingEventsByGroupKey(group), Double.MIN_VALUE, Double.MAX_VALUE, offset, limit));
                        break;
                    case PROCESSED:
                        eventUuids.addAll(jedis.zrangeByScore(keyNaming.processedEventsByGroupKey(group), Double.MIN_VALUE, Double.MAX_VALUE, offset, limit));
                        break;
                    case ERROR:
                        eventUuids.addAll(jedis.zrangeByScore(keyNaming.failedEventsByGroupKey(group), Double.MIN_VALUE, Double.MAX_VALUE, offset, limit));
                        break;
                }
            }

            final List<UUID> uuids = eventUuids.stream().map(UUID::fromString).collect(Collectors.toList());
            return new ListResult<>(offset, limit, uuids.size(), uuids);
        }
    }


    private static List<Statuses> resendableStatuses = Arrays.asList(Statuses.ERROR, Statuses.PROCESSING);

    @Override
    public void resendEvent(UUID eventUuid, String eventGroup) {

        if (eventUuid == null)
            throw new IllegalArgumentException("eventUuid should not be null");
        if (eventGroup == null)
            throw new IllegalArgumentException("eventGroup should not be null");

        try (Jedis jedis = pool.getResource()) {

            final String groupEventDataKey = keyNaming.eventDetailsKey(eventGroup, eventUuid);
            final String groupEventStatusStr = jedis.hget(groupEventDataKey, STATUS.name());

            if (groupEventStatusStr == null) {
                throw new IllegalArgumentException("Event group with uuid " + eventUuid + " and group name " + eventGroup + " does not exist");
            }

            final Statuses status = Statuses.valueOf(groupEventStatusStr);

            if (!resendableStatuses.contains(status)) {
                throw new IllegalArgumentException("Event status [" + status + "] is not resendable [" + resendableStatuses + "]");
            }

            final String removeKey;

            if (Statuses.ERROR.equals(status)) {
                removeKey = keyNaming.failedEventsByGroupKey(eventGroup);
            } else if (Statuses.PROCESSING.equals(status)) {
                removeKey = keyNaming.processingEventsByGroupKey(eventGroup);
            } else throw new RuntimeException("Ilyen eset nem lehet!");

            //tranzakciót nyitok, törlöm a régi sorból, hozzáadom a CREATED sorhoz, update-elem a státuszát
            final Transaction tx = jedis.multi();
            tx.zrem(removeKey, eventUuid.toString());
            DataHandling.zaddNow(tx, keyNaming.unprocessedEventsByGroupKey(eventGroup), eventUuid.toString());
            tx.hset(groupEventDataKey, STATUS.name(), Statuses.CREATED.name());
            tx.exec();
        }
    }

    @Override
    public void removeEvent(UUID eventUuid, String eventGroup) {

        if (eventUuid == null)
            throw new IllegalArgumentException("eventUuid should not be null");
        if (eventGroup == null)
            throw new IllegalArgumentException("eventGroup should not be null");

        try (Jedis jedis = pool.getResource()) {

            final String groupEventDataKey = keyNaming.eventDetailsKey(eventGroup, eventUuid);
            final String groupEventStatusStr = jedis.hget(groupEventDataKey, STATUS.name());

            if (groupEventStatusStr == null) {
                throw new IllegalArgumentException("Event group with uuid " + eventUuid + " and group name " + eventGroup + " does not exist");
            }

            final Statuses status = Statuses.valueOf(groupEventStatusStr);

            final String removeKey;

            if (Statuses.ERROR.equals(status)) {
                removeKey = keyNaming.failedEventsByGroupKey(eventGroup);
            } else if (Statuses.PROCESSING.equals(status)) {
                removeKey = keyNaming.processingEventsByGroupKey(eventGroup);
            } else if (Statuses.PROCESSED.equals(status)) {
                removeKey = keyNaming.processedEventsByGroupKey(eventGroup);
            } else if (Statuses.CREATED.equals(status)) {
                removeKey = keyNaming.unprocessedEventsByGroupKey(eventGroup);
            } else throw new RuntimeException("Ilyen eset nem lehet!");

            jedis.zrem(removeKey, eventUuid.toString());
            jedis.del(groupEventDataKey);

            //TODO: az adatelem törlése, ha nem kapcsolódik rá több group
        }
    }
}
