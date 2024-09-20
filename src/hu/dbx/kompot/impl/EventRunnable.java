package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.async.EventFrame;
import hu.dbx.kompot.consumer.async.EventReceivingCallback;
import hu.dbx.kompot.consumer.async.EventStatusCallback;
import hu.dbx.kompot.core.MessagingService;
import hu.dbx.kompot.impl.consumer.ConsumerConfig;
import hu.dbx.kompot.impl.consumer.ConsumerHandlers;
import org.slf4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

// csokkenti a folyamatban levo eventek szamat.
public final class EventRunnable implements Runnable {

    private final ConsumerImpl consumer;
    private final ConsumerConfig consumerConfig;
    private final AtomicInteger processingEvents;
    private final ConsumerHandlers consumerHandlers;
    private final Object message;
    private final List<EventReceivingCallback> eventReceivingCallbacks;
    private final MessagingService messagingService;
    private final UUID messageUuid;

    private static final Logger LOGGER = LoggerUtils.getLogger();

    public EventRunnable(ConsumerImpl consumer, AtomicInteger processingEvents, ConsumerHandlers consumerHandlers, Object message, List<EventReceivingCallback> eventReceivingCallbacks) {
        this.consumer = consumer;
        this.consumerConfig = consumer.getConsumerConfig();
        this.processingEvents = processingEvents;
        this.consumerHandlers = consumerHandlers;
        this.message = message;
        this.eventReceivingCallbacks = eventReceivingCallbacks;
        this.messagingService = consumerConfig.getMessagingService();
        this.messageUuid = messagingService.getMessageUuid(message);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        try {
            final EventStatusCallback callback = messagingService.getEventStatusCallback(message, eventReceivingCallbacks);

            final Optional<EventFrame<?>> frame;

            try {
                frame = messagingService.getEventFrame(message, consumerConfig, consumerHandlers);
            } catch (Exception e) {
                try {
                    callback.error(e);
                } catch (RuntimeException ee) {
                    LOGGER.error("Could not write error state back to redis, eventUuid=" + messageUuid, e);
                }
                messagingService.afterEvent(consumer, processingEvents, consumerHandlers, eventReceivingCallbacks);
                return;
            }

            if (!frame.isPresent()) {
                messagingService.afterEvent(consumer, processingEvents, consumerHandlers, eventReceivingCallbacks);
                return;
            }

            callback.setFrame(frame.get());

            callback.markProcessing();
            LOGGER.trace("Started processing event uuid={}", messageUuid);
            LOGGER.info("Processing event {}/{} meta={} uuid={}", frame.get().getSourceIdentifier(),
                    frame.get().getEventMarker().getEventName(), frame.get().getMetaData(), messageUuid);

            for (EventReceivingCallback eventReceivingCallback : eventReceivingCallbacks) {
                try {
                    eventReceivingCallback.onEventReceived(frame.get());
                } catch (Throwable t) {
                    LOGGER.error("Error executing callback on event uuid=" + messageUuid, t);
                }
            }

            consumer.getEventProcessorAdapter().handle(frame.get().getEventMarker(), frame.get().getMetaData(), frame.get().getEventData(), callback);

            LOGGER.info("Processed event {}/{} uuid={}", frame.get().getSourceIdentifier(), frame.get().getEventMarker().getEventName(), messageUuid);

            // az a gond, hogy a processingEventsCounter a finally-ban csokkentve lesz, de itt nem.
            messagingService.afterEvent(consumer, processingEvents, consumerHandlers, eventReceivingCallbacks);

        } catch (Throwable t) {
            LOGGER.error("Error during handing event=" + messageUuid, t);

            throw t;
        }
    }
}
