package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.async.EventReceivingCallback;
import hu.dbx.kompot.impl.consumer.ConsumerConfig;
import hu.dbx.kompot.impl.consumer.ConsumerHandlers;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Minden esemeny feldolgozasa uta lefut es korulnez az esemenyek kozott, mit tudna meg feldolgozni.
 */
final class AfterEventRunnable implements ConsumerImpl.Trampoline {

    private static final Logger LOGGER = LoggerUtils.getLogger();

    private final ConsumerImpl consumer;
    private final ConsumerConfig consumerConfig;
    private final AtomicInteger processingEvents;
    private final ConsumerHandlers consumerHandlers;
    private final List<EventReceivingCallback> eventReceivingCallbacks;

    AfterEventRunnable(ConsumerImpl consumer, ConsumerConfig consumerConfig, AtomicInteger processingEvents, ConsumerHandlers consumerHandlers, List<EventReceivingCallback> eventReceivingCallbacks) {
        this.consumer = consumer;
        this.consumerConfig = consumerConfig;
        this.processingEvents = processingEvents;
        this.consumerHandlers = consumerHandlers;
        this.eventReceivingCallbacks = eventReceivingCallbacks;
    }

    @Override
    public ConsumerImpl.Trampoline jump() {
        final String groupCode = consumer.getConsumerIdentity().getEventGroup();
        final String dbKey = consumer.getKeyNaming().unprocessedEventsByGroupKey(groupCode);

        try (final Jedis store = consumerConfig.getPool().getResource()) {
            final Set<String> elems = store.zrangeByScore(dbKey, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 1);

            if (elems != null && !elems.isEmpty()) {
                final UUID eventUuid = UUID.fromString(elems.iterator().next());
                return new EventRunnable(consumer, consumerConfig, processingEvents, consumerHandlers, eventUuid, eventReceivingCallbacks);
            }
        } catch (Throwable t) {
            LOGGER.error("Error on automatic event processing: ", t);
        }
        return null;
    }
}
