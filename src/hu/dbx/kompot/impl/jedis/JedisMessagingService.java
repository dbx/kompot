package hu.dbx.kompot.impl.jedis;

import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.Listener;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.EventFrame;
import hu.dbx.kompot.consumer.async.EventReceivingCallback;
import hu.dbx.kompot.consumer.async.EventStatusCallback;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.consumer.sync.MethodRequestFrame;
import hu.dbx.kompot.core.MessagingService;
import hu.dbx.kompot.core.SerializeHelper;
import hu.dbx.kompot.events.Priority;
import hu.dbx.kompot.exceptions.DeserializationException;
import hu.dbx.kompot.exceptions.MessageErrorResultException;
import hu.dbx.kompot.impl.ConsumerImpl;
import hu.dbx.kompot.impl.DataHandling;
import hu.dbx.kompot.impl.EventRunnable;
import hu.dbx.kompot.impl.LoggerUtils;
import hu.dbx.kompot.impl.consumer.ConsumerConfig;
import hu.dbx.kompot.impl.consumer.ConsumerHandlers;
import hu.dbx.kompot.impl.jedis.status.SelfStatusWriter;
import hu.dbx.kompot.impl.producer.ProducerConfig;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static hu.dbx.kompot.impl.DataHandling.EventKeys.STATUS;
import static hu.dbx.kompot.impl.DataHandling.MethodResponseKeys.RESPONSE;
import static hu.dbx.kompot.impl.DataHandling.Statuses;
import static hu.dbx.kompot.impl.jedis.JedisHelper.*;

public class JedisMessagingService implements MessagingService {

    private static final Logger LOGGER = LoggerUtils.getLogger();

    private final JedisPool pool;
    private final ConsumerIdentity consumerIdentity;
    private final KeyNaming keyNaming;
    private ThreadSafePubSub pubSub;

    public JedisMessagingService(JedisPool pool, ConsumerIdentity consumerIdentity, KeyNaming keyNaming) {
        this.pool = pool;
        this.consumerIdentity = consumerIdentity;
        this.keyNaming = keyNaming;
    }

    @Override
    public void start(Listener listener, Set<String> supportedBroadcastCodes) throws InterruptedException {
        this.pubSub = new ThreadSafePubSub(pool, listener);
        pubSub.startWithChannels(consumerIdentity, supportedBroadcastCodes);
    }

    @Override
    public void stop() throws InterruptedException {
        pubSub.unsubscrubeAllAndStop();
    }

    @Override
    public void afterStarted(ConsumerImpl consumer, AtomicInteger processingEvents, ConsumerHandlers consumerHandlers, List<EventReceivingCallback> eventReceivingCallbacks) {
        SelfStatusWriter.start(consumer.getConsumerConfig());

        // There may be unprocessed events in the queue so we start to process it on as many threads as possible
        for (int i = 1; i < consumer.getConsumerConfig().getMaxEventThreadCount() + 1; i++) {
            // csak azert noveljuk, mert a runNextEvent csokkenteni fogja!
            processingEvents.incrementAndGet();

            // inditunk egy feldolgozast, hatha
            runNextEvent(consumer, processingEvents, consumerHandlers, eventReceivingCallbacks);
        }
    }

    @Override
    public void afterStopped(ConsumerConfig consumerConfig) {
        SelfStatusWriter.delete(consumerConfig);
    }

    @Override
    public void sendEvent(EventDescriptor<?> marker, EventFrame<?> eventFrame, ProducerConfig producerConfig) {
        final Priority priority = marker.getPriority();

        try (Jedis jedis = pool.getResource()) {
            Transaction transaction = jedis.multi();
            // save event data contents.
            saveEventDetails(transaction, keyNaming, eventFrame.getEventGroups(), eventFrame, producerConfig.getProducerIdentity());

            // register item in each group queue.
            saveEventGroups(transaction, keyNaming, eventFrame.getIdentifier(), priority, eventFrame.getEventGroups());

            // publish on pubsub
            LOGGER.trace("Publishing pubsub on {}", eventFrame.debugSignature());
            eventFrame.getEventGroups().forEach(group -> transaction.publish("e:" + group, eventFrame.getIdentifier().toString()));

            transaction.exec();
            LOGGER.trace("Called exec on {}", eventFrame.debugSignature());
        }
    }

    @Override
    public void sendMessage(MethodRequestFrame<?> requestFrame, ProducerConfig producerConfig) {
        try (final Jedis jedis = pool.getResource()) {
            final Transaction transaction = jedis.multi();
            // bementjuk a memoriaba
            writeMethodFrame(transaction, keyNaming, requestFrame);

            // publikaljuk a metodust!
            final String methodGroup = requestFrame.getMethodMarker().getMethodGroupName();

            // megszolitjuk a cel modult
            final String channel = "m:" + methodGroup;
            transaction.publish(channel, requestFrame.getIdentifier().toString());
            LOGGER.trace("Published on channel {}", channel);

            transaction.exec();
            LOGGER.trace("Called exec on {}", requestFrame.debugSignature());
        }
    }

    @Override
    public void broadcast(BroadcastDescriptor<?> descriptor, String serializedData) {
        try (Jedis jedis = pool.getResource()) {
            final String channel = "b:" + descriptor.getBroadcastCode();
            // LOGGER.trace(getProducerIdentity().getIdentifier() + "Broadcasting on channel" + channel);
            jedis.publish(channel, serializedData);
            // LOGGER.trace(getProducerIdentity().getIdentifier() + "Did broadcast on channel " + channel);
        }
    }

    @Override
    public EventStatusCallback getEventStatusCallback(Object message, List<EventReceivingCallback> eventReceivingCallbacks) {
        return new JedisEventStatusCallback(pool, getMessageUuid(message), keyNaming, consumerIdentity, eventReceivingCallbacks);
    }

    @Override
    public void afterEvent(ConsumerImpl consumer, AtomicInteger processingEvents, ConsumerHandlers consumerHandlers, List<EventReceivingCallback> eventReceivingCallbacks) {
        runNextEvent(consumer, processingEvents, consumerHandlers, eventReceivingCallbacks);
    }

    @Override
    public Optional<EventFrame<?>> getEventFrame(Object message, ConsumerConfig consumerConfig, ConsumerHandlers consumerHandlers) throws Exception {
        final UUID eventUuid = getMessageUuid(message);

        try (final Jedis store = pool.getResource()) {

            final String groupCode = consumerIdentity.getEventGroup();
            // megprobaljuk ellopni

            long result = store.hsetnx(keyNaming.eventDetailsKey(groupCode, eventUuid), "owner", consumerIdentity.getIdentifier());

            if (result == 0) {
                LOGGER.trace("Some other instance of {} has already gathered evt {}", groupCode, eventUuid);

                // we remove event here also, so that if the db gents inconsistent then we do not loop on the same value.
                store.zrem(keyNaming.unprocessedEventsByGroupKey(groupCode), eventUuid.toString());

                return Optional.empty();
            } else {
                // itt a versenyhelyzet elkerulese miatt remove van. ha ezt kiszedjuk, megnonek a logok.
                store.zrem(keyNaming.unprocessedEventsByGroupKey(groupCode), eventUuid.toString());

                try {
                    return Optional.of(JedisHelper.readEventFrame(store, keyNaming, consumerHandlers.getEventResolver(),
                            eventUuid, consumerConfig.getLogSensitiveDataKeys()));
                } catch (IllegalArgumentException e) {
                    // did not find event details under kiven key in db
                    LOGGER.error(e.getMessage() + ", event-uuid=" + eventUuid);
                    // we do not persist error to db because event uuid likely does not exist is redis.
                    throw e;
                } catch (IllegalStateException e) {
                    // could not find marker for given event code
                    LOGGER.error(e.getMessage() + ", event-uuid=" + eventUuid);
                    throw e;
                }
            }
        } catch (DeserializationException e) {
            LOGGER.error("Could not deserialize event data", e);
            throw e;
        }
    }

    @Override
    public Optional<Statuses> getMethodStatus(Object message, ProducerConfig producerConfig, MethodRequestFrame<?> requestFrame) {
        try (final Jedis jedis = pool.getResource()) {
            return methodStatus(jedis, keyNaming, requestFrame.getIdentifier());
        }
    }

    @Override
    public MessageErrorResultException getMethodError(Object message, ProducerConfig producerConfig, MethodRequestFrame<?> requestFrame) {
        try (final Jedis jedis = pool.getResource()) {
            final String methodDetailsKey = keyNaming.methodDetailsKey(requestFrame.getIdentifier());
            return deserializeException(methodDetailsKey, jedis);
        }
    }

    @Override
    public byte[] getMethodResponse(Object message, ProducerConfig producerConfig, MethodRequestFrame<?> requestFrame) {
        try (final Jedis jedis = pool.getResource()) {
            final String methodDetailsKey = keyNaming.methodDetailsKey(requestFrame.getIdentifier());
            return jedis.hget(methodDetailsKey.getBytes(), RESPONSE.name().getBytes());
        }
    }

    @Override
    public Optional<MethodRequestFrame<?>> getMethodRequestFrame(Object message, ConsumerConfig consumerConfig, ConsumerHandlers consumerHandlers) throws Exception {
        final UUID methodUuid = getMessageUuid(message);
        try (final Jedis jedis = pool.getResource()) {
            if (!steal(jedis, methodUuid)) {
                LOGGER.debug("Could not steal {}", methodUuid);
                // some other instance has already took this item, we do nothing
                return Optional.empty();
            }
            return JedisHelper.readMethodFrame(jedis, keyNaming,
                    consumerHandlers.getMethodDescriptorResolver(), methodUuid, consumerConfig.getLogSensitiveDataKeys());
        }
    }

    /**
     * Tries to mark method as being processed by current instance. Returns true iff succeeded.
     */
    private boolean steal(Jedis store, UUID methodUuid) {
        final String methodKey = keyNaming.methodDetailsKey(methodUuid);
        LOGGER.trace("Trying to steal from {}", methodUuid);
        return 0 != store.hsetnx(methodKey, "owner", consumerIdentity.getIdentifier());
    }

    /**
     * Sets timeout on method key and notifies reqester module.
     */
    @Override
    public void sendMethodRespond(Object response, Object message, MethodRequestFrame<?> mrf, Throwable throwable) {
        if (throwable != null) {
            LOGGER.error("Hiba a metodus feldolgozasa kozben!", throwable);
            try (final Jedis jedis = pool.getResource()) {
                final String methodKey = keyNaming.methodDetailsKey(getMessageUuid(message));
                final Transaction tx = jedis.multi();
                tx.hset(methodKey, DataHandling.MethodResponseKeys.STATUS.name(), "ERROR");
                tx.hset(methodKey, DataHandling.MethodResponseKeys.EXCEPTION_CLASS.name(), throwable.getClass().getName());
                if (throwable.getMessage() != null) {
                    tx.hset(methodKey, DataHandling.MethodResponseKeys.EXCEPTION_MESSAGE.name(), throwable.getMessage());
                }
                tx.exec();
            }
        }

        if (mrf == null) {
            return;
        }

        final UUID senderUuid = UUID.fromString(mrf.getSourceIdentifier());

        try (final Jedis jedis = pool.getResource()) {
            if (!jedis.getClient().isConnected()) {
                LOGGER.error("Redis got disconnected. exiting.");
            } else if (jedis.getClient().isBroken()) {
                LOGGER.error("Client got broken. exiting.");
            } else {
                final String methodKey = keyNaming.methodDetailsKey(getMessageUuid(message));

                if (throwable == null) {
                    //noinspection rawtypes
                    final MethodDescriptor methodMarker = mrf.getMethodMarker();
                    jedis.zrem(keyNaming.unprocessedEventsByGroupKey(methodMarker.getMethodGroupName()), mrf.getIdentifier().toString());
                    // TODO: use multi/exec here to writre statuses and stuff.
                    jedis.hset(methodKey.getBytes(), DataHandling.MethodResponseKeys.RESPONSE.name().getBytes(), SerializeHelper.compressData(response));
                    jedis.hset(methodKey, DataHandling.MethodResponseKeys.STATUS.name(), DataHandling.Statuses.PROCESSED.name());
                }

                // hogy nehogy lejarjon mire megjon a valasz!
                jedis.expire(methodKey, 15);

                final String responseNotificationChannel = "id:" + senderUuid;

                LOGGER.debug("Notifying response on {} with {}", responseNotificationChannel, getMessageUuid(message));

                jedis.publish(responseNotificationChannel, getMessageUuid(message).toString());
            }
        }
    }

    @Override
    public UUID getMessageUuid(Object message) {
        try {
            return UUID.fromString((String) message);
        } catch (Exception e) {
            LOGGER.error("Could not get message uuid from message ({}) {}", message, e);
            return null;
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

    private void runNextEvent(ConsumerImpl consumer, AtomicInteger processingEvents, ConsumerHandlers consumerHandlers, List<EventReceivingCallback> eventReceivingCallbacks) {
        final Optional<UUID> eventUuid = findNextEvent();
        if (eventUuid.isPresent()) {
            submitToExecutor(consumer.getConsumerConfig(), new EventRunnable(consumer, processingEvents, consumerHandlers, eventUuid.get().toString(), eventReceivingCallbacks));
        } else {
            final int afterDecrement = processingEvents.decrementAndGet();
            if (afterDecrement < 0) {
                throw new IllegalStateException("Processing Events counter must not ever get negative: " + afterDecrement);
            }
        }
    }

    private Optional<UUID> findNextEvent() {
        final String groupCode = consumerIdentity.getEventGroup();
        final String dbKey = keyNaming.unprocessedEventsByGroupKey(groupCode);

        try (final Jedis store = pool.getResource()) {
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

    private void submitToExecutor(ConsumerConfig consumerConfig, Runnable runnable) {
        consumerConfig.getExecutor().execute(runnable);
    }

    public JedisPool getPool() {
        return pool;
    }

    public KeyNaming getKeyNaming() {
        return keyNaming;
    }

}
