package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.async.EventFrame;
import hu.dbx.kompot.consumer.async.EventReceivingCallback;
import hu.dbx.kompot.exceptions.DeserializationException;
import hu.dbx.kompot.impl.consumer.ConsumerConfig;
import hu.dbx.kompot.impl.consumer.ConsumerHandlers;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

// csokkenti a folyamatban levo eventek szamat.
final class EventRunnable implements ConsumerImpl.Trampoline {
    private final ConsumerImpl consumer;
    private final ConsumerConfig consumerConfig;
    private final AtomicInteger processingEvents;
    private final ConsumerHandlers consumerHandlers;
    private final UUID eventUuid;
    private final List<EventReceivingCallback> eventReceivingCallbacks;

    private static final Logger LOGGER = LoggerUtils.getLogger();

    EventRunnable(ConsumerImpl consumer, ConsumerConfig consumerConfig, AtomicInteger processingEvents, ConsumerHandlers consumerHandlers, UUID uuid, List<EventReceivingCallback> eventReceivingCallbacks) {
        this.consumer = consumer;
        this.consumerConfig = consumerConfig;
        this.processingEvents = processingEvents;
        this.consumerHandlers = consumerHandlers;
        this.eventUuid = uuid;
        this.eventReceivingCallbacks = eventReceivingCallbacks;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ConsumerImpl.Trampoline jump() {
        try {
            final DefaultCallback callback = new DefaultCallback(consumerConfig.getPool(), eventUuid, consumer.getKeyNaming(), consumer.getConsumerIdentity(), eventReceivingCallbacks);

            final EventFrame frame;
            try (final Jedis store = consumerConfig.getPool().getResource()) {

                final String groupCode = consumer.getConsumerIdentity().getEventGroup();
                // megprobaljuk ellopni

                long result = store.hsetnx(consumer.getKeyNaming().eventDetailsKey(groupCode, eventUuid), "owner", consumer.getConsumerIdentity().getIdentifier());

                if (result == 0) {
                    LOGGER.trace("Some other instance of {} has already gathered evt {}", groupCode, eventUuid);

                    // we remove event here also, so that if the db gents inconsistent then we do not loop on the same value.
                    store.zrem(consumer.getKeyNaming().unprocessedEventsByGroupKey(groupCode), eventUuid.toString());

                    return new AfterEventRunnable(consumer, consumerConfig, processingEvents, consumerHandlers, eventReceivingCallbacks);
                } else {
                    // itt a versenyhelyzet elkerulese miatt remove van. ha ezt kiszedjuk, megnonek a logok.
                    store.zrem(consumer.getKeyNaming().unprocessedEventsByGroupKey(groupCode), eventUuid.toString());

                    try {
                        frame = DataHandling.readEventFrame(store, consumer.getKeyNaming(), consumerHandlers.getEventResolver(), eventUuid);
                        callback.setFrame(frame);
                    } catch (IllegalArgumentException e) {
                        // did not find event details under kiven key in db
                        LOGGER.error(e.getMessage() + ", event-uuid=" + eventUuid);
                        // we do not persist error to db because event uuid likely does not exist is redis.
                        return new AfterEventRunnable(consumer, consumerConfig, processingEvents, consumerHandlers, eventReceivingCallbacks);
                    } catch (IllegalStateException e) {
                        // could not find marker for given event code
                        LOGGER.error(e.getMessage() + ", event-uuid=" + eventUuid);

                        try {
                            callback.error(e);
                        } catch (RuntimeException ee) {
                            LOGGER.error("Could not write error state back to redis, eventUuid=" + eventUuid, e);
                        }
                        return new AfterEventRunnable(consumer, consumerConfig, processingEvents, consumerHandlers, eventReceivingCallbacks);
                    }
                }
            } catch (DeserializationException e) {
                LOGGER.error("Could not deserialize event data", e);
                callback.error(e);
                return new AfterEventRunnable(consumer, consumerConfig, processingEvents, consumerHandlers, eventReceivingCallbacks);
            }

            callback.markProcessing();
            LOGGER.trace("Started processing event uuid={}", eventUuid);
            LOGGER.info("Processing event {}/{} meta={} uuid={}", frame.getSourceIdentifier(), frame.getEventMarker().getEventName(), frame.getMetaData(), eventUuid);

            for (EventReceivingCallback eventReceivingCallback : eventReceivingCallbacks) {
                try {
                    eventReceivingCallback.onEventReceived(frame);
                } catch (Throwable t) {
                    LOGGER.error("Error executing callback on event uuid=" + eventUuid, t);
                }
            }

            consumer.getEventProcessorAdapter().handle(frame.getEventMarker(), frame.getMetaData(), frame.getEventData(), callback);

            LOGGER.info("Processed event {}/{} uuid={}", frame.getSourceIdentifier(), frame.getEventMarker().getEventName(), eventUuid);

            // az a gond, hogy a processingEventsCounter a finally-ban csokkentve lesz, de itt nem.
            return new AfterEventRunnable(consumer, consumerConfig, processingEvents, consumerHandlers, eventReceivingCallbacks);
        } catch (Throwable t) {
            LOGGER.error("Error during handing event=" + eventUuid, t);

            throw t;
        }
    }
}
