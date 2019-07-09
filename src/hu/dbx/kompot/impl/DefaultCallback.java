package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventFrame;
import hu.dbx.kompot.consumer.async.EventReceivingCallback;
import hu.dbx.kompot.consumer.async.EventStatusCallback;
import hu.dbx.kompot.core.KeyNaming;
import hu.dbx.kompot.events.Priority;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static hu.dbx.kompot.impl.DataHandling.EventKeys.PRIORITY;
import static hu.dbx.kompot.impl.LoggerUtils.debugEventFrame;

/**
 * Default error handling strategy.
 * Maintains statuses in redis keys ands calls event receiving callbacks.
 */
final class DefaultCallback implements EventStatusCallback {
    private final UUID eventId;
    private final JedisPool pool;
    private final AtomicBoolean hasBeenCalled = new AtomicBoolean(false);
    private final KeyNaming keyNaming;
    private final ConsumerIdentity consumerIdentity;

    private final AtomicReference<EventFrame> frame = new AtomicReference<>();
    private final List<EventReceivingCallback> eventReceivingCallbacks;

    private static final Logger LOGGER = LoggerUtils.getLogger();

    // TODO: nem jo h redis peldanayt kap, mert thread-safe-nek kellene lennie!
    DefaultCallback(JedisPool pool, UUID eventId, KeyNaming keyNaming, ConsumerIdentity consumerIdentity, List<EventReceivingCallback> eventReceivingCallbacks) {
        this.pool = pool;
        this.eventId = eventId;
        this.keyNaming = keyNaming;
        this.consumerIdentity = consumerIdentity;
        this.eventReceivingCallbacks = eventReceivingCallbacks;
    }

    /**
     * Marks event as being in processing state.
     */
    void markProcessing() {
        writeStatus(DataHandling.Statuses.PROCESSING);
    }

    void setFrame(EventFrame frame) {
        assert frame != null;
        this.frame.set(frame);
    }

    @Override
    public void success(String message) {
        if (hasBeenCalled.getAndSet(true))
            throw new IllegalStateException("Callback has been called once!");

        writeStatus(DataHandling.Statuses.PROCESSED);

        // success callbacks
        for (EventReceivingCallback eventReceivingCallback : eventReceivingCallbacks) {
            try {
                eventReceivingCallback.onEventProcessedSuccessfully(frame.get(), message);
            } catch (Throwable t) {
                debugEventFrame(LOGGER, frame.get());
                LOGGER.error("Error executing callback on event uuid=" + eventId, t);
            }
        }
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

        // failure callbacks
        for (EventReceivingCallback eventReceivingCallback : eventReceivingCallbacks) {
            try {
                eventReceivingCallback.onEventProcessingFailure(frame.get(), e);
            } catch (Throwable t) {
                debugEventFrame(LOGGER, frame.get());
                LOGGER.error("Error executing callback on event uuid=" + eventId, t);
            }
        }

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
        } finally {
            LOGGER.trace("Written status {} for event", status);
        }
    }
}
