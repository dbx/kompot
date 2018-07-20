package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventStatusCallback;
import hu.dbx.kompot.core.KeyNaming;
import hu.dbx.kompot.events.Priority;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static hu.dbx.kompot.impl.DataHandling.EventKeys.PRIORITY;

/**
 * Default error handling strategy.
 * Writes statuses to history keys.
 */
final class DefaultCallback implements EventStatusCallback {
    private final UUID eventId;
    private final JedisPool pool;
    private final AtomicBoolean hasBeenCalled = new AtomicBoolean(false);
    private final KeyNaming keyNaming;
    private final ConsumerIdentity consumerIdentity;

    private static final Logger LOGGER = LoggerUtils.getLogger();

    // TODO: nem jo h redis peldanayt kap, mert thread-safe-nek kellene lennie!
    DefaultCallback(JedisPool pool, UUID eventId, KeyNaming keyNaming, ConsumerIdentity consumerIdentity) {
        this.pool = pool;
        this.eventId = eventId;
        this.keyNaming = keyNaming;
        this.consumerIdentity = consumerIdentity;
    }

    // megjeloli az esemenyt peldanyt feldolgozas alattira
    void markProcessing() {
        writeStatus(DataHandling.Statuses.PROCESSING);
    }

    @Override
    public void success(String message) {
        if (hasBeenCalled.getAndSet(true))
            throw new IllegalStateException("Callback has been called once!");

        writeStatus(DataHandling.Statuses.PROCESSED);
    }

    @Override
    public void error(String e) {
        error(new RuntimeException(e));
    }

    @Override
    public void error(Throwable e) {
        if (hasBeenCalled.getAndSet(true))
            throw new IllegalStateException("Callback has been called once!");

        writeStatus(DataHandling.Statuses.ERROR);
    }

    private void writeStatus(DataHandling.Statuses status) {
        LOGGER.debug("Writing status {} for event {}/{} of consumer {}", status, eventId, consumerIdentity.getEventGroup(), consumerIdentity.getIdentifier());

        final String groupName = consumerIdentity.getEventGroup();
        final String eventGroupDetailsKey = keyNaming.eventDetailsKey(groupName, eventId);

        try (Jedis jedis = pool.getResource()) {
            final Priority priority = Priority.valueOf(jedis.hget(keyNaming.eventDetailsKey(eventId), PRIORITY.name()));

            final Transaction multi = jedis.multi();
            multi.hset(eventGroupDetailsKey, DataHandling.EventKeys.STATUS.name(), status.name());

            if (status == DataHandling.Statuses.PROCESSING) {
                multi.zrem(keyNaming.unprocessedEventsByGroupKey(groupName), eventId.toString());
                DataHandling.zaddNow(multi, keyNaming.processingEventsByGroupKey(groupName), priority, eventId.toString().getBytes());
            } else if (status == DataHandling.Statuses.ERROR) {
                multi.zrem(keyNaming.processingEventsByGroupKey(groupName), eventId.toString());
                DataHandling.zaddNow(multi, keyNaming.failedEventsByGroupKey(groupName), priority, eventId.toString().getBytes());
            } else if (status == DataHandling.Statuses.PROCESSED) {
                multi.zrem(keyNaming.processingEventsByGroupKey(groupName), eventId.toString());
                DataHandling.zaddNow(multi, keyNaming.processedEventsByGroupKey(groupName), priority, eventId.toString().getBytes());
                DataHandling.decrementUnprocessedGroupsCounter(multi, keyNaming, eventId);
            }

            multi.exec();
        }
    }
}
