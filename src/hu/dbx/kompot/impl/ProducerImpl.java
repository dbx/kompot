package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.EventFrame;
import hu.dbx.kompot.consumer.async.EventSendingCallback;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.consumer.sync.MethodRequestFrame;
import hu.dbx.kompot.consumer.sync.MethodSendingCallback;
import hu.dbx.kompot.core.SerializeHelper;
import hu.dbx.kompot.exceptions.DeserializationException;
import hu.dbx.kompot.exceptions.MessageErrorResultException;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.producer.ProducerConfig;
import hu.dbx.kompot.moby.MetaDataHolder;
import hu.dbx.kompot.producer.EventGroupProvider;
import hu.dbx.kompot.producer.Producer;
import hu.dbx.kompot.producer.ProducerIdentity;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static hu.dbx.kompot.impl.DataHandling.Statuses;
import static hu.dbx.kompot.impl.DataHandling.Statuses.*;

public final class ProducerImpl implements Producer {
    private static final Logger LOGGER = LoggerUtils.getLogger();

    private final EventGroupProvider provider;

    private final List<MethodSendingCallback> methodEventListeners = new LinkedList<>();
    private final List<EventSendingCallback> eventSendingEventListeners = new LinkedList<>();
    private final ConsumerImpl consumer;

    private final ProducerConfig producerConfig;

    public ProducerImpl(ProducerConfig producerConfig, EventGroupProvider provider, ConsumerImpl consumer) {
        this.provider = provider;
        this.consumer = consumer;
        this.producerConfig = producerConfig;
    }

    @Override
    public <TReq> void sendEvent(EventDescriptor<TReq> marker, TReq request, MetaDataHolder metaData) {
        if (null == marker) {
            throw new NullPointerException("Event marker is null!");
        } else if (null == request) {
            throw new NullPointerException("Request object is null for marker of: " + marker.getEventName());
        }

        LOGGER.info("Sending event {} with meta {}", marker.getEventName(), metaData);

        final Iterable<String> eventGroups = getEventGroupProvider().findEventGroups(marker);
        final EventFrame<TReq> eventFrame = EventFrame.build(marker, request, metaData, eventGroups);

        eventSendingEventListeners.forEach(eventListener -> {
            try {
                eventListener.beforeEventSent(eventFrame);
            } catch (Throwable t) {
                LOGGER.error("Exception when handling beforeEventSent on listener \" + eventListener + \" for " + eventFrame.debugSignature(), t);
            }
        });

        producerConfig.getMessagingService().sendEvent(marker, eventFrame, producerConfig);
        LOGGER.info("Sent event {}", marker.getEventName());

        eventSendingEventListeners.forEach(eventListener -> {
            try {
                eventListener.onEventSent(eventFrame);
            } catch (Throwable t) {
                LOGGER.error("Exception when handling onEventSent on listener \" + eventListener + \" for " + eventFrame.debugSignature(), t);
            }
        });
    }

    @Override
    public EventGroupProvider getEventGroupProvider() {
        return provider;
    }

    @Override
    public <TReq, TRes> CompletableFuture<TRes> sendMessage(MethodDescriptor<TReq, TRes> marker, TReq methodData, MetaDataHolder metaData) throws SerializationException {
        if (marker == null) {
            throw new IllegalArgumentException("Can not send async message for null marker!");
        } else if (methodData == null) {
            throw new IllegalArgumentException("Can not send message " + marker.getMethodName() + " for empty data!");
        }

        LOGGER.info("Sending method {} with meta {}", marker.getMethodName(), metaData);

        // make request object
        final MethodRequestFrame<TReq> requestFrame = MethodRequestFrame.build(consumer.getConsumerIdentity(), marker, methodData, metaData);

        LOGGER.trace("Built method to send: {}", requestFrame);

        final CompletableFuture<TRes> responseFuture = new CompletableFuture<>();

        //  felregisztralunk a valasz objektumra
        consumer.registerMessageFuture(requestFrame.getIdentifier(), message -> messageCallback(message, requestFrame, responseFuture));

        //timeout beállítása
        scheduleMethodTimeout(requestFrame, responseFuture, marker.getTimeout());

        producerConfig.getMessagingService().sendMessage(requestFrame, producerConfig);
        LOGGER.info("Sent message {}", marker.getMethodName());

        methodEventListeners.forEach(methodEventListener -> {
            try {
                methodEventListener.onRequestSent(requestFrame);
            } catch (Throwable t) {
                LOGGER.error("Exception when handling onRequestSent callback of " + requestFrame.debugSignature(), t);
            }
        });

        return responseFuture;
    }

    /**
     * Arra valo, hogy ha idon belul nem erkezik meg a valasz, akkor a CompletableFuture peldanyt megjeloli lejartkent (cancel).
     */
    private <TReq, TRes> void scheduleMethodTimeout(MethodRequestFrame<TReq> requestFrame, CompletableFuture<TRes> responseFuture, long timeoutMs) {
        producerConfig.getScheduledExecutor().schedule(() -> {
            if (!responseFuture.isCancelled() && !responseFuture.isCompletedExceptionally() && !responseFuture.isDone()) {
                if (responseFuture.cancel(false)) {
                    methodEventListeners.forEach(methodEventListener -> {
                        try {
                            methodEventListener.onTimeOut(requestFrame);
                        } catch (Throwable t) {
                            LOGGER.error("Exception when handling onTimeOut callback of " + requestFrame.debugSignature(), t);
                        }
                    });
                }
            }
        }, timeoutMs, TimeUnit.MILLISECONDS);
    }

    private <TReq, TRes> void messageCallback(Object message, MethodRequestFrame<TReq> requestFrame, CompletableFuture<TRes> response) {
        //noinspection EmptyFinallyBlock
        try {
            Optional<Statuses> i = producerConfig.getMessagingService().getMethodStatus(message, producerConfig, requestFrame);
            if (!i.isPresent()) {
                LOGGER.warn("Could not find status for message {}. Maybe already processed?", requestFrame.getIdentifier());
            } else if (ERROR.equals(i.get())) {
                methodError(message, requestFrame, response);
            } else if (PROCESSED.equals(i.get())) {
                methodProcessed(message, requestFrame, response);
            } else if (PROCESSING.equals(i.get())) {// itt mar vissza kellett legyen irva az esemeny feldolgozottsaganak allapota
                final String msg = "The method should not be in PROCESSING state! frame=" + requestFrame.debugSignature();
                throw new IllegalStateException(msg);
            }
        } catch (DeserializationException e) {
            response.completeExceptionally(e);
        } finally {
            // TODO: torojuk rediszbol ami ott maradt.
        }
    }

    /**
     * Kezel egy hibas statuszura allitott metodust
     */
    private <TReq, TRes> void methodError(Object message, MethodRequestFrame<TReq> requestFrame, CompletableFuture<TRes> response) {
        final MessageErrorResultException exception = producerConfig.getMessagingService().getMethodError(message, producerConfig, requestFrame);
        response.completeExceptionally(exception);

        methodEventListeners.forEach(methodEventListener -> {
            try {
                methodEventListener.onErrorReceived(requestFrame, exception);
            } catch (Throwable t) {
                LOGGER.error("Error handling onErrorReceived callback of " + requestFrame.debugSignature(), t);
            }
        });
    }

    /**
     * Kezel egy feldolgozott statuszura allitott metodust
     */
    private <TReq, TRes> void methodProcessed(Object message,
                                              MethodRequestFrame<TReq> requestFrame,
                                              CompletableFuture<TRes> response) throws DeserializationException {
        final byte[] data = producerConfig.getMessagingService().getMethodResponse(message, producerConfig, requestFrame);

        //noinspection unchecked
        final TRes res = (TRes) decompressMethodResponse(data, requestFrame.getMethodMarker());
        methodEventListeners.forEach(methodEventListener -> {
            try {
                methodEventListener.onResponseReceived(requestFrame, res);
            } catch (Throwable t) {
                LOGGER.error("Error handling requestSent event!", t);
            }
        });
        response.complete(res);
    }

    private static Object decompressMethodResponse(byte[] methodDataZip, MethodDescriptor marker) throws DeserializationException {
        Object requestData;
        try (ByteArrayInputStream input = new ByteArrayInputStream(methodDataZip); GZIPInputStream iz = new GZIPInputStream(input)) {
            requestData = SerializeHelper.deserializeResponse(iz, marker);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return requestData;
    }


    public void addMethodSendingCallback(MethodSendingCallback listener) throws IllegalArgumentException {
        if (listener == null) {
            throw new IllegalArgumentException("Method sending event listener must not be null!");
        } else {
            methodEventListeners.add(listener);
        }
    }

    public void removeMethodSendingCallback(MethodSendingCallback listener) throws IllegalArgumentException {
        if (listener == null) {
            throw new IllegalArgumentException("Method sending event listener must not be null!");
        } else {
            methodEventListeners.remove(listener);
        }
    }

    public void addEventSendingCallback(EventSendingCallback listener) throws IllegalArgumentException {
        if (listener == null) {
            throw new IllegalArgumentException("Event sending event listener must not be null!");
        } else {
            eventSendingEventListeners.add(listener);
        }
    }

    public void removeEventSendingCallback(EventSendingCallback listener) throws IllegalArgumentException {
        if (listener == null) {
            throw new IllegalArgumentException("Event sending event listener must not be null!");
        } else {
            eventSendingEventListeners.remove(listener);
        }
    }

    @Override
    public <TReq> void broadcast(BroadcastDescriptor<TReq> descriptor,
                                 TReq broadcastData) throws SerializationException {
        final String serializedData = SerializeHelper.serializeObject(broadcastData);
        producerConfig.getMessagingService().broadcast(descriptor, serializedData);
    }

    public void shutdown() {
        producerConfig.getScheduledExecutor().shutdown();
    }

    @Override
    public ProducerIdentity getProducerIdentity() {
        return producerConfig.getProducerIdentity();
    }
}