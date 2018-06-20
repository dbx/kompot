package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.EventFrame;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.consumer.sync.MethodRequestFrame;
import hu.dbx.kompot.consumer.sync.MethodSendingEventListener;
import hu.dbx.kompot.core.KeyNaming;
import hu.dbx.kompot.core.SerializeHelper;
import hu.dbx.kompot.exceptions.DeserializationException;
import hu.dbx.kompot.exceptions.MessageErrorResultException;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.producer.EventGroupProvider;
import hu.dbx.kompot.producer.Producer;
import hu.dbx.kompot.producer.ProducerIdentity;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static hu.dbx.kompot.core.SerializeHelper.deserializeException;
import static hu.dbx.kompot.core.SerializeHelper.deserializeResponse;
import static hu.dbx.kompot.impl.DataHandling.EventKeys.STATUS;
import static hu.dbx.kompot.impl.DataHandling.MethodResponseKeys.RESPONSE;
import static hu.dbx.kompot.impl.DataHandling.*;
import static java.util.UUID.randomUUID;

public final class ProducerImpl implements Producer, ProducerIdentity {
    private static final Logger LOGGER = LoggerUtils.getLogger();

    private final EventGroupProvider provider;
    private final KeyNaming keyNaming;
    private final String identifier = randomUUID().toString();
    private final ScheduledExecutorService methodTimeoutExecutor = Executors.newSingleThreadScheduledExecutor();
    private final List<MethodSendingEventListener> methodEventListeners = new LinkedList<>();
    private final JedisPool jedisPool;
    private final ConsumerImpl consumer;

    public ProducerImpl(JedisPool pool, EventGroupProvider provider, KeyNaming keyNaming, ConsumerImpl consumer) {
        if (provider == null || pool == null || keyNaming == null)
            throw new NullPointerException("Arguments must not be null!");

        this.provider = provider;
        this.jedisPool = pool;
        this.keyNaming = keyNaming;
        this.consumer = consumer;
    }

    @Override
    public <TReq> void sendEvent(EventDescriptor<TReq> marker, TReq request) throws SerializationException {
        if (null == marker)
            throw new NullPointerException("Event marker is null!");
        if (null == request)
            throw new NullPointerException("Request object is null for marker: " + marker);

        final EventFrame<TReq> eventFrame = EventFrame.build(marker, request);
        final Iterable<String> eventGroups = getEventGroupProvider().findEventGroups(marker);

        try (Jedis jedis = jedisPool.getResource()) {
            Transaction transaction = jedis.multi();
            // save event data contents.
            saveEventDetails(transaction, keyNaming, eventGroups, eventFrame, getProducerIdentity());

            // register item in each group queue.
            saveEventGroups(transaction, keyNaming, eventFrame.getIdentifier(), eventGroups);

            // publish on pubsub
            transaction.publish("e:" + marker.getEventName(), eventFrame.getIdentifier().toString());
            transaction.exec();
        }
    }


    @Override
    public EventGroupProvider getEventGroupProvider() {
        return provider;
    }

    @Override
    public <TReq, TRes> CompletableFuture<TRes> sendMessage(MethodDescriptor<TReq, TRes> marker, TReq methodData, long timeoutMs) throws SerializationException {
        if (marker == null)
            throw new IllegalArgumentException("Can not send async message for null marker!");
        if (methodData == null)
            throw new IllegalArgumentException("Can not send message " + marker.getMethodName() + " for empty data!");

        // make request object
        final MethodRequestFrame<TReq> requestFrame = MethodRequestFrame.build(this, marker, methodData);

        LOGGER.trace("Built method to send: {}", requestFrame);

        final CompletableFuture<TRes> responseFuture = new CompletableFuture<>();

        try (Jedis jedis = jedisPool.getResource()) {

            // bementjuk a memoriaba
            writeMethodFrame(jedis, keyNaming, requestFrame);

            // publikaljuk a metodust!
            final String methodGroup = requestFrame.getMethodMarker().getMethodGroupName();

            //  felregisztralunk a valasz objektumra
            consumer.registerMessageFuture(getProducerIdentity(), requestFrame.getIdentifier(), () -> messageCallback(requestFrame, responseFuture));

            //timeout beállítása
            scheduleMethodTimeout(requestFrame, responseFuture, timeoutMs);

            // megszolitjuk a cel modult
            String channel = "m:" + methodGroup;
            jedis.publish(channel, requestFrame.getIdentifier().toString());
            LOGGER.trace("Published on channel {}", channel);
        }

        for (MethodSendingEventListener methodEventListener : methodEventListeners) {
            try {
                methodEventListener.onRequestSent(requestFrame);
            } catch (Throwable t) {
                LOGGER.error("Error handling requestSent event!", t);
            }
        }

        return responseFuture;
    }

    /**
     * Arra valo, hogy ha idon belul nem erkezik meg a valasz, akkor a CompletableFuture peldanyt megjeloli lejartkent (cancel).
     */
    private <TReq, TRes> void scheduleMethodTimeout(MethodRequestFrame<TReq> requestFrame, CompletableFuture<TRes> responseFuture, long timeoutMs) {
        methodTimeoutExecutor.schedule(() -> {
                    if (!responseFuture.isCancelled() && !responseFuture.isCompletedExceptionally() && !responseFuture.isDone()) {
                        for (MethodSendingEventListener methodEventListener : methodEventListeners) {
                            try {
                                methodEventListener.onTimeOut(requestFrame);
                            } catch (Throwable t) {
                                LOGGER.error("Error handling requestSent event!", t);
                            }
                        }
                        responseFuture.cancel(false);
                    }
                }
                , timeoutMs, TimeUnit.MILLISECONDS);
    }

    private <TReq, TRes> void messageCallback(MethodRequestFrame<TReq> requestFrame, CompletableFuture<TRes> response) {
        try (Jedis jds = jedisPool.getResource()) {

            final String methodDetailsKey = keyNaming.methodDetailsKey(requestFrame.getIdentifier());

            // TODO: legyen multi/exec!
            final String status = jds.hget(methodDetailsKey, STATUS.name());
            final String data = jds.hget(methodDetailsKey, RESPONSE.name());
            try {
                switch (DataHandling.Statuses.valueOf(status)) {
                    case ERROR:
                        final MessageErrorResultException exception = deserializeException(methodDetailsKey, jds);
                        response.completeExceptionally(exception);
                        break;
                    case PROCESSED:
                        //noinspection unchecked
                        response.complete((TRes) deserializeResponse(data, requestFrame.getMethodMarker()));
                        break;
                    case PROCESSING:
                        throw new IllegalStateException("The event should not be in PROCESSED state");
                }

            } catch (DeserializationException e) {
                response.completeExceptionally(e);
            } finally {
                // TODO: torojuk rediszbol ami ott maradt.
            }
        }
    }

    @SuppressWarnings("unused")
    public void addMethodEventListener(MethodSendingEventListener listener) {
        this.methodEventListeners.add(listener);
    }

    @SuppressWarnings("unused")
    public void removeMethodEventListener(MethodSendingEventListener listener) {
        this.methodEventListeners.remove(listener);
    }

    @Override
    public <TReq> void broadcast(BroadcastDescriptor<TReq> descriptor, TReq broadcastData) throws SerializationException {
        try (Jedis jedis = jedisPool.getResource()) {
            String serializedData = SerializeHelper.serializeObject(broadcastData);
            String channel = "b:" + descriptor.getBroadcastCode();
            LOGGER.info(getProducerIdentity().getIdentifier() + "Broadcasting on channel" + channel);
            jedis.publish(channel, serializedData);
            LOGGER.info(getProducerIdentity().getIdentifier() + "Did broadcast!");
        }
    }

    public void shutdown() {
        methodTimeoutExecutor.shutdown();
    }

    @Override
    public ProducerIdentity getProducerIdentity() {
        return this;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }
}
