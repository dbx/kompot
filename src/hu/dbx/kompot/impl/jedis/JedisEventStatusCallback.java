package hu.dbx.kompot.impl.jedis;

import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventFrame;
import hu.dbx.kompot.consumer.async.EventReceivingCallback;
import hu.dbx.kompot.events.Priority;
import hu.dbx.kompot.impl.DataHandling;
import hu.dbx.kompot.impl.DefaultEventStatusCallback;
import hu.dbx.kompot.impl.LoggerUtils;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static hu.dbx.kompot.impl.DataHandling.EventKeys.PRIORITY;

/**
 * Default error handling strategy.
 * Maintains statuses in redis keys ands calls event receiving callbacks.
 */
final class JedisEventStatusCallback extends DefaultEventStatusCallback {

    private final UUID eventId;
    private final JedisPool pool;
    private final KeyNaming keyNaming;
    private final ConsumerIdentity consumerIdentity;
    private final AtomicReference<EventFrame> frame = new AtomicReference<>();

    private static final Logger LOGGER = LoggerUtils.getLogger();

    // TODO: nem jo h redis peldanayt kap, mert thread-safe-nek kellene lennie!
    JedisEventStatusCallback(JedisPool pool, UUID eventId, KeyNaming keyNaming, ConsumerIdentity consumerIdentity, List<EventReceivingCallback> eventReceivingCallbacks) {
        super(eventId, eventReceivingCallbacks);
        this.pool = pool;
        this.eventId = eventId;
        this.keyNaming = keyNaming;
        this.consumerIdentity = consumerIdentity;
    }

    /**
     * Marks event as being in processing state.
     */
    @Override
    public void markProcessing() {
        writeStatus(DataHandling.Statuses.PROCESSING);
    }

    @Override
    public void success(String message) {
        super.success(message);
        writeStatus(DataHandling.Statuses.PROCESSED);
    }

    @Override
    public void error(Throwable e) {
        super.error(e);
        writeStatus(DataHandling.Statuses.ERROR);
    }

    private void writeStatus(final DataHandling.Statuses status) {
//        LOGGER.debug("Writing status {} for event {}/{} of consumer {}", status, eventId, consumerIdentity.getEventGroup(), consumerIdentity.getIdentifier());

        final String groupName = consumerIdentity.getEventGroup();
        final String eventGroupDetailsKey = keyNaming.eventDetailsKey(groupName, eventId);

        try (Jedis jedis = pool.getResource()) {
            final String priorityValue = jedis.hget(keyNaming.eventDetailsKey(eventId), PRIORITY.name());

            if (priorityValue == null) {
                // unknown case. can not write error status because event details are is missing.
                LOGGER.error("Could not write status {} for eventId={}/{}", status, eventId, consumerIdentity.getEventGroup());
                return;
            }

            final Priority priority = Priority.valueOf(priorityValue);

            final Transaction multi = jedis.multi();
            multi.hset(eventGroupDetailsKey, DataHandling.EventKeys.STATUS.name(), status.name());

            if (status == DataHandling.Statuses.PROCESSING) {
                multi.zrem(keyNaming.unprocessedEventsByGroupKey(groupName), eventId.toString());
                JedisHelper.zaddNow(multi, keyNaming.processingEventsByGroupKey(groupName), priority, eventId.toString().getBytes());
            } else if (status == DataHandling.Statuses.ERROR) {
                multi.zrem(keyNaming.processingEventsByGroupKey(groupName), eventId.toString());
                JedisHelper.zaddNow(multi, keyNaming.failedEventsByGroupKey(groupName), priority, eventId.toString().getBytes());
            } else if (status == DataHandling.Statuses.PROCESSED) {
                multi.zrem(keyNaming.processingEventsByGroupKey(groupName), eventId.toString());
                JedisHelper.zaddNow(multi, keyNaming.processedEventsByGroupKey(groupName), priority, eventId.toString().getBytes());
                JedisHelper.decrementUnprocessedGroupsCounter(multi, keyNaming, eventId);
            }

            multi.exec();
        } finally {
            LOGGER.trace("Written status {} for event", status);
        }
    }
}
