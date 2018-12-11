package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.EventFrame;
import hu.dbx.kompot.consumer.async.EventSendingCallback;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.consumer.sync.MethodRequestFrame;
import hu.dbx.kompot.consumer.sync.MethodSendingCallback;
import hu.dbx.kompot.core.KeyNaming;
import hu.dbx.kompot.core.SerializeHelper;
import hu.dbx.kompot.events.Priority;
import hu.dbx.kompot.exceptions.DeserializationException;
import hu.dbx.kompot.exceptions.MessageErrorResultException;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.producer.ProducerConfig;
import hu.dbx.kompot.moby.MetaDataHolder;
import hu.dbx.kompot.producer.EventGroupProvider;
import hu.dbx.kompot.producer.Producer;
import hu.dbx.kompot.producer.ProducerIdentity;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static hu.dbx.kompot.core.SerializeHelper.deserializeException;
import static hu.dbx.kompot.core.SerializeHelper.deserializeResponse;
import static hu.dbx.kompot.impl.DataHandling.EventKeys.STATUS;
import static hu.dbx.kompot.impl.DataHandling.MethodResponseKeys.RESPONSE;
import static hu.dbx.kompot.impl.DataHandling.*;
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
    public <TReq> void sendEvent(EventDescriptor<TReq> marker, TReq request, MetaDataHolder metaData) throws SerializationException {
        if (null == marker) {
            throw new NullPointerException("Event marker is null!");
        } else if (null == request) {
            throw new NullPointerException("Request object is null for marker of: " + marker.getEventName());
        }

        final EventFrame<TReq> eventFrame = EventFrame.build(marker, request, metaData);
        final Iterable<String> eventGroups = getEventGroupProvider().findEventGroups(marker);
        final Priority priority = marker.getPriority();

        try (Jedis jedis = producerConfig.getPool().getResource()) {
            Transaction transaction = jedis.multi();
            // save event data contents.
            saveEventDetails(transaction, producerConfig.getNaming(), eventGroups, eventFrame, getProducerIdentity());

            // register item in each group queue.
            saveEventGroups(transaction, producerConfig.getNaming(), eventFrame.getIdentifier(), priority, eventGroups);

            // publish on pubsub
            LOGGER.trace("Publishing pubsub on {}", eventFrame.debugSignature());
            eventGroups.forEach(group -> transaction.publish("e:" + group, eventFrame.getIdentifier().toString()));

            transaction.exec();
            LOGGER.debug("Called exec on {}", eventFrame.debugSignature());
        }

        eventSendingEventListeners.forEach(eventListener -> {
            try {
                eventListener.onEventSent(eventFrame);
            } catch (Throwable t) {
                LOGGER.error("Exception when handling onEventSent event for " + eventFrame.debugSignature(), t);
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

        // make request object
        final MethodRequestFrame<TReq> requestFrame = MethodRequestFrame.build(getProducerIdentity(), marker, methodData, metaData);

        LOGGER.trace("Built method to send: {}", requestFrame);

        final CompletableFuture<TRes> responseFuture = new CompletableFuture<>();

        try (final Jedis jedis = producerConfig.getPool().getResource()) {
            final Transaction transaction = jedis.multi();

            // bementjuk a memoriaba
            writeMethodFrame(transaction, producerConfig.getNaming(), requestFrame);

            // publikaljuk a metodust!
            final String methodGroup = requestFrame.getMethodMarker().getMethodGroupName();

            //  felregisztralunk a valasz objektumra
            consumer.registerMessageFuture(requestFrame.getIdentifier(), () -> messageCallback(requestFrame, responseFuture));

            //timeout beállítása
            scheduleMethodTimeout(requestFrame, responseFuture, marker.getTimeout());

            // megszolitjuk a cel modult
            final String channel = "m:" + methodGroup;
            transaction.publish(channel, requestFrame.getIdentifier().toString());
            LOGGER.trace("Published on channel {}", channel);

            transaction.exec();
        }

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

    private <TReq, TRes> void messageCallback(MethodRequestFrame<TReq> requestFrame, CompletableFuture<TRes> response) {
        //noinspection EmptyFinallyBlock
        try (final Jedis jedis = producerConfig.getPool().getResource()) {
            Optional<Statuses> i = methodStatus(jedis, producerConfig.getNaming(), requestFrame.getIdentifier());
            if (!i.isPresent()) {
                LOGGER.warn("Could not find status for message {}. Maybe already processed?", requestFrame.getIdentifier());
            } else if (ERROR.equals(i.get())) {
                methodError(producerConfig.getNaming(), requestFrame, response, jedis);
            } else if (PROCESSED.equals(i.get())) {
                methodProcessed(producerConfig.getNaming(), requestFrame, response, jedis);
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
     * Visszaadja egy metodushivas statuszat uuid alapjan.
     *
     * @param jedis      jedis kapcsolat
     * @param keyNaming  kulcs nevezesek
     * @param methodUuid nem null metodus azonosito
     * @return statusz objektum ami soha nem null
     */
    private Optional<Statuses> methodStatus(Jedis jedis, KeyNaming keyNaming, UUID methodUuid) {
        final String methodDetailsKey = keyNaming.methodDetailsKey(methodUuid);
        String statusString = jedis.hget(methodDetailsKey, STATUS.name());
        if (statusString == null || statusString.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(DataHandling.Statuses.valueOf(statusString));
        }
    }

    /**
     * Kezel egy hibas statuszura allitott metodust
     */
    private <TReq, TRes> void methodError(KeyNaming keyNaming, MethodRequestFrame<TReq> requestFrame, CompletableFuture<TRes> response, Jedis jds) {
        final String methodDetailsKey = keyNaming.methodDetailsKey(requestFrame.getIdentifier());
        final MessageErrorResultException exception = deserializeException(methodDetailsKey, jds);
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
    private <TReq, TRes> void methodProcessed(KeyNaming keyNaming,
                                              MethodRequestFrame<TReq> requestFrame,
                                              CompletableFuture<TRes> response,
                                              Jedis jedis) throws DeserializationException {
        final String methodDetailsKey = keyNaming.methodDetailsKey(requestFrame.getIdentifier());
        final String data = jedis.hget(methodDetailsKey, RESPONSE.name());

        //noinspection unchecked
        final TRes res = (TRes) deserializeResponse(data, requestFrame.getMethodMarker());
        methodEventListeners.forEach(methodEventListener -> {
            try {
                methodEventListener.onResponseReceived(requestFrame, res);
            } catch (Throwable t) {
                LOGGER.error("Error handling requestSent event!", t);
            }
        });
        response.complete(res);
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
        try (Jedis jedis = producerConfig.getPool().getResource()) {
            final String serializedData = SerializeHelper.serializeObject(broadcastData);
            final String channel = "b:" + descriptor.getBroadcastCode();
            // LOGGER.trace(getProducerIdentity().getIdentifier() + "Broadcasting on channel" + channel);
            jedis.publish(channel, serializedData);
            // LOGGER.trace(getProducerIdentity().getIdentifier() + "Did broadcast on channel " + channel);
        }
    }

    public void shutdown() {
        producerConfig.getScheduledExecutor().shutdown();
    }

    @Override
    public ProducerIdentity getProducerIdentity() {
        return producerConfig.getProducerIdentity();
    }
}