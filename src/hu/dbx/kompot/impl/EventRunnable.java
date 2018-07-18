package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.async.EventFrame;
import hu.dbx.kompot.exceptions.DeserializationException;
import hu.dbx.kompot.impl.consumer.ConsumerConfig;
import hu.dbx.kompot.impl.consumer.ConsumerHandlers;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

// csokkenti a folyamatban levo eventek szamat.
final class EventRunnable implements ConsumerImpl.Trampoline {
    private ConsumerImpl consumer;
    private final ConsumerConfig consumerConfig;
    private final AtomicInteger processingEvents;
    private final ConsumerHandlers consumerHandlers;
    private final UUID eventUuid;

    private static final Logger LOGGER = LoggerUtils.getLogger();


    EventRunnable(ConsumerImpl consumer, ConsumerConfig consumerConfig, AtomicInteger processingEvents, ConsumerHandlers consumerHandlers, UUID uuid) {
        this.consumer = consumer;
        this.consumerConfig = consumerConfig;
        this.processingEvents = processingEvents;
        this.consumerHandlers = consumerHandlers;
        this.eventUuid = uuid;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ConsumerImpl.Trampoline jump() {
        try {
            final DefaultCallback callback = new DefaultCallback(consumerConfig.getPool(), eventUuid, consumer.getKeyNaming(), consumer.getConsumerIdentity());

            final EventFrame frame;
            try (final Jedis store = consumerConfig.getPool().getResource()) {

                final String groupCode = consumer.getConsumerIdentity().getEventGroup();
                // megprobaljuk ellopni

                long result = store.hsetnx(consumer.getKeyNaming().eventDetailsKey(groupCode, eventUuid), "owner", consumer.getConsumerIdentity().getIdentifier());

                if (result == 0) {
                    LOGGER.trace("Some other instance of {} has already gathered evt {}", groupCode, eventUuid);
                    return new AfterEventRunnable(consumer, consumerConfig, processingEvents, consumerHandlers);
                } else {
                    // itt a versenyhelyzet elkerulese miatt remove van. ha ezt kiszedjuk, megnonek a logok.
                    store.zrem(consumer.getKeyNaming().unprocessedEventsByGroupKey(groupCode), eventUuid.toString());
                    frame = DataHandling.readEventFrame(store, consumer.getKeyNaming(), consumerHandlers.getEventResolver(), eventUuid);
                }
            } catch (DeserializationException e) {
                LOGGER.error("Could not deserialize event data", e);
                callback.error(e);
                return new AfterEventRunnable(consumer, consumerConfig, processingEvents, consumerHandlers);
            }

            callback.markProcessing();
            LOGGER.debug("Sterted processing event uuid={}", eventUuid);
            consumer.getEventProcessorAdapter().handle(frame.getEventMarker(), frame.getMetaData(), frame.getEventData(), callback);

            LOGGER.debug("Processed event uuid={}", eventUuid);
            consumerConfig.getExecutor().execute(new ConsumerImpl.TrampolineRunner(new AfterEventRunnable(consumer, consumerConfig, processingEvents, consumerHandlers)));

            // TODO: itt neki kellene allni feldolgozni mas, beragadt esemenyeket is.
        } finally {
            processingEvents.decrementAndGet();
        }
        return new AfterEventRunnable(consumer, consumerConfig, processingEvents, consumerHandlers);
    }
}
