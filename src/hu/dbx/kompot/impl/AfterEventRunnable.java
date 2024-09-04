package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.async.EventReceivingCallback;
import hu.dbx.kompot.impl.consumer.ConsumerConfig;
import hu.dbx.kompot.impl.consumer.ConsumerHandlers;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Optional;
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

    @SuppressWarnings("OptionalIsPresent")
    @Override
    public ConsumerImpl.Trampoline jump() {
        final Optional<UUID> eventUuid = findNextEvent();
        if (eventUuid.isPresent()) {
            return new EventRunnable(consumer, consumerConfig, processingEvents, consumerHandlers, eventUuid.get(), eventReceivingCallbacks);
        } else {
            final int afterDecrement = processingEvents.decrementAndGet();
            if (afterDecrement < 0) {
                throw new IllegalStateException("Processing Events counter must not ever get negative: " + afterDecrement);
            }
            return null;
        }
    }

    private Optional<UUID> findNextEvent() {
        final String groupCode = consumer.getConsumerIdentity().getEventGroup();
        final String dbKey = consumer.getKeyNaming().unprocessedEventsByGroupKey(groupCode);

        try (final Jedis store = consumerConfig.getPool().getResource()) {
            return Optional.of(store)
                    //TODO: Redis 6.2-től ZRANGEBYSCORE deprecated, helyette ZRANGE használható
                    .map(s -> s.zrangeByScore(dbKey, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 1))
                    .flatMap(e -> e.stream().findFirst())
                    .map(UUID::fromString);
        } catch (Throwable t) {
            LOGGER.error("Error on finding next event!", t);
            return Optional.empty();
        }
    }
}
