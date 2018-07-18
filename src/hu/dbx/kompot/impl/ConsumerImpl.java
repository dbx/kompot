package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.Consumer;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventFrame;
import hu.dbx.kompot.consumer.async.handler.EventProcessorAdapter;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastProcessorFactory;
import hu.dbx.kompot.consumer.broadcast.handler.SelfDescribingBroadcastProcessor;
import hu.dbx.kompot.consumer.sync.MethodReceivingCallback;
import hu.dbx.kompot.consumer.sync.MethodRequestFrame;
import hu.dbx.kompot.consumer.sync.handler.DefaultMethodProcessorAdapter;
import hu.dbx.kompot.core.KeyNaming;
import hu.dbx.kompot.core.SerializeHelper;
import hu.dbx.kompot.exceptions.DeserializationException;
import hu.dbx.kompot.impl.consumer.ConsumerConfig;
import hu.dbx.kompot.impl.consumer.ConsumerHandlers;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Transaction;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public final class ConsumerImpl implements Consumer, Runnable {

    /**
     * Maximum number of event processing threads.
     * TODO: make it configurable!
     */
    private static final int MAX_EVENTS = 4;
    private static final Logger LOGGER = LoggerUtils.getLogger();

    /**
     * Number of event processors in progress.
     */
    private final AtomicInteger processingEvents = new AtomicInteger(0);
    private final ConsumerHandlers consumerHandlers;
    private final ConsumerConfig consumerConfig;

    private final List<MethodReceivingCallback> methodEventListeners = Collections.synchronizedList(new LinkedList<>());

    public ConsumerImpl(ConsumerConfig consumerConfig, ConsumerHandlers consumerHandlers) {
        this.consumerConfig = consumerConfig;
        this.consumerHandlers = consumerHandlers;
    }

    private final Map<UUID, Runnable> futures = new ConcurrentHashMap<>();

    /**
     * Felregisztral egy future peldanyt es var a valaszra.
     * TODO: timeout parameter is legyen!
     */
    void registerMessageFuture(UUID messageUuid, Runnable runnable) {
        String messageResponseChannel = consumerConfig.getNaming().getMessageResponseNotificationChannel(messageUuid);
        LOGGER.debug("Subscribing to {}", messageResponseChannel);
        pubSub.subscribe(messageResponseChannel);
        futures.put(messageUuid, runnable);
    }

    private final CountDownLatch startLatch = new CountDownLatch(1);

    private final Thread daemonThread = new Thread(this);

    private final JedisPubSub pubSub = new JedisPubSub() {

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            if (channel.startsWith("id:")) {
                startLatch.countDown();
            }
        }

        @Override
        public void onMessage(String channel, String message) {
            // itt ki kell talalni h kinek adom tovabb.
            if (channel.startsWith("b:")) {
                final String broadcastCode = channel.substring(2);
                LOGGER.info("Received Broadcast of code {} for {}", broadcastCode, consumerConfig.getConsumerIdentity().getIdentifier());
                consumerConfig.getExecutor().execute(new BroadcastRunnable(broadcastCode, message));
            } else if (channel.startsWith("e:")) {
                startEventProcessing(UUID.fromString(message));
            } else if (channel.startsWith("m:")) {                 // uzenet keres
                LOGGER.debug("Received message bang {} on channel {}, trying to start method.", message, channel);
                //noinspection unused
                final String methodName = channel.substring(2);
                try {
                    consumerConfig.getExecutor().execute(new MethodRunnable(UUID.fromString(message)));
                } catch (RejectedExecutionException rejected) {
                    LOGGER.error("Could not start execution, executor service rejected. maybe too much?");
                }
            } else if (channel.contains(":r:")) {
                LOGGER.debug("Receiving method response: {} => {}", channel, message);
                futures.remove(UUID.fromString(message)).run();
            } else {
                LOGGER.error("Unexpected message on channel {} => {}", channel, message);
            }
        }
    };

    // TODO: how to stop it when undeployed?
    // TODO: do not use threading here.
    public void startDaemonThread() throws InterruptedException {
        daemonThread.start();
        startLatch.await();

        // we start processing earlier events.
        consumerConfig.getExecutor().execute(new TrampolineRunner(new AfterEventRunnable()));
    }

    public void shutdown() {
        pubSub.unsubscribe();
    }

    @Override
    public ConsumerIdentity getConsumerIdentity() {
        return consumerConfig.getConsumerIdentity();
    }

    @Override
    public KeyNaming getKeyNaming() {
        return consumerConfig.getNaming();
    }

    @Override
    public EventProcessorAdapter getEventProcessorAdapter() {
        return consumerHandlers.getEventProcessorAdapter();
    }

    @Override
    public DefaultMethodProcessorAdapter getMethodProcessorAdapter() {
        return consumerHandlers.getMethodProcessorAdapter();
    }

    @Override
    public BroadcastProcessorFactory getBroadcastProcessorAdapter() {
        return consumerHandlers.getBroadcastProcessorFactory();
    }

    /**
     * Osszeszedi az osszes figyelt csatornat.
     */
    private String[] getPubSubChannels() {
        List<String> channels = new LinkedList<>();
        channels.add("e:" + getConsumerIdentity().getEventGroup());
        channels.add("m:" + getConsumerIdentity().getMessageGroup());

        consumerHandlers.getBroadcastProcessorFactory().getSupportedBroadcasts().forEach(b -> channels.add("b:" + b.getBroadcastCode()));
        channels.add("id:" + getConsumerIdentity().getIdentifier());

        return channels.toArray(new String[]{});
    }

    @Override
    public void run() {
        try (Jedis jedis = consumerConfig.getPool().getResource()) {
            LOGGER.info("Subscribing to pubsub channels: {} on {}", getPubSubChannels(), getConsumerIdentity().getIdentifier());

            jedis.subscribe(pubSub, getPubSubChannels());
            // TODO: ezt a szalat is le kell tudni allitani!!!
            LOGGER.info("Exiting pubsub listener thread of {}", getConsumerIdentity().getIdentifier());
        } catch (Throwable e) {
            LOGGER.error("Exception on pubsub listener thread of " + getConsumerIdentity().getIdentifier(), e);
        } finally {
            LOGGER.info("Quitting pubsub listener thread of {}", getConsumerIdentity().getIdentifier());
        }
    }

    private final class BroadcastRunnable implements Runnable {
        private final String broadcastCode;
        private final String data;

        private BroadcastRunnable(String broadcastCode, String data) {
            this.broadcastCode = broadcastCode;
            this.data = data;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void run() {
            final Optional<BroadcastDescriptor> descriptor = consumerHandlers.getBroadcastDescriptorResolver().resolveMarker(broadcastCode);
            if (!descriptor.isPresent()) {
                LOGGER.error("Did not find descriptor for broadcast code {}", broadcastCode);
                return;
            }
            try {
                LOGGER.debug("Deserializing broadcast data...");
                final Object dataObj = SerializeHelper.deserializeBroadcast(consumerHandlers.getBroadcastDescriptorResolver(), broadcastCode, data);
                LOGGER.debug("Deserialized broadcast data of type {}", dataObj.getClass());

                Optional<SelfDescribingBroadcastProcessor> factory = consumerHandlers.getBroadcastProcessorFactory().create(descriptor.get());
                if (!factory.isPresent()) {
                    // ez elvileg nem lehetseges, mert csak azokra iratkozunk fel, amikre tudunk is figyelni.
                    LOGGER.error("Illegalis allapot, nincsen broadcast a keresett '{}' tipusra!", broadcastCode);
                } else {
                    LOGGER.debug("Handling broadcast {}", broadcastCode);
                    factory.get().handle(dataObj);
                    LOGGER.debug("Successfully handled broadcast {}", broadcastCode);
                }
            } catch (DeserializationException e) {
                LOGGER.error("Could not deserialize broadcast payload for code {} and data {}", broadcastCode, data);
            }
        }
    }

    private final class MethodRunnable implements Runnable {

        private final UUID methodUuid;

        private MethodRunnable(UUID methodUuid) {
            this.methodUuid = methodUuid;
        }

        @Override
        public void run() {
            try (final Jedis store = consumerConfig.getPool().getResource()) {
                // try to assign this method to self

                // a metodus cucca ide megy.
                final String methodKey = getKeyNaming().methodDetailsKey(methodUuid);

                // ebben fogom tarolni a metodus hivas reszleteit.
                MethodRequestFrame frame = null;

                LOGGER.debug("Trying to steal from {}", methodKey);
                Long result = store.hsetnx(methodKey, "owner", getConsumerIdentity().getIdentifier());

                if (result == 0) {
                    LOGGER.debug("Could not steal {}", methodKey);
                    // some other instance has already took this item, we do nothing
                    return;
                }

                // itt egy masik try-catch van, mert csak akkor irhatom vissza, hogy nem sikerult, ha mar egyem az ownership.
                try {
                    final Optional<MethodRequestFrame> frameOp = DataHandling.readMethodFrame(store, getKeyNaming(), consumerHandlers.getMethodDescriptorResolver(), methodUuid);

                    if (!frameOp.isPresent()) {
                        LOGGER.debug("Could not read from method {}", methodKey);
                        // lejart a metodus?
                        return;
                    }

                    frame = frameOp.get();
                    final MethodRequestFrame mrf = frame;

                    store.zrem(getKeyNaming().unprocessedEventsByGroupKey(frame.getMethodMarker().getMethodGroupName()), frame.getIdentifier().toString());
                    // esemenykezelok futtatasa
                    methodEventListeners.forEach(x -> {
                        try {
                            x.onRequestReceived(mrf);
                        } catch (Throwable t) {
                            LOGGER.error("Error when running method sending event listener {} for method {}", x, methodUuid);
                        }
                    });

                    LOGGER.debug("Calling method processor");
                    // siker eseten visszairjuk a sikeres vackot

                    // TODO: irjuk be a folyamatban levo esemenyes soraba!
                    //noinspection unchecked
                    final Object response = getMethodProcessorAdapter().call(frame.getMethodMarker(), frame.getMethodData(), frame.getMetaData());
                    LOGGER.debug("Called method processor");

                    // TODO: use multi/exec here to write statuses and stuff.
                    store.hset(methodKey, DataHandling.MethodResponseKeys.RESPONSE.name(), SerializeHelper.serializeObject(response));
                    store.hset(methodKey, DataHandling.MethodResponseKeys.STATUS.name(), DataHandling.Statuses.PROCESSED.name());

                    // esemenykezelok futtatasa
                    methodEventListeners.forEach(x -> {
                        try {
                            x.onRequestProcessedSuccessfully(mrf, response);
                        } catch (Throwable t) {
                            LOGGER.error("Error when running method sending event listener for {}", t);
                        }
                    });

                    LOGGER.debug("Written response stuff");
                } catch (Throwable t) {
                    // TODO: irjuk be a hibas esemenyek soraba!

                    writeMethodFailure(store, methodKey, t);

                    if (frame != null) {
                        final MethodRequestFrame mrf = frame;
                        // esemenykezelok futtatasa
                        methodEventListeners.forEach(x -> {
                            try {
                                x.onRequestProcessingFailure(mrf, t);
                            } catch (Throwable e) {
                                LOGGER.error("Error when running method sending event listener for {} on {}", e);
                            }
                        });
                    }

                } finally {
                    // hogy nehogy lejarjon mire megjon a valasz!
                    store.expire(methodKey, 15);

                    String responseNotificationChannel = getKeyNaming().getMessageResponseNotificationChannel(methodUuid);
                    LOGGER.debug("Notifying responging staff on {}", responseNotificationChannel);

                    store.publish(responseNotificationChannel, methodUuid.toString());
                }
            }
        }

        private void writeMethodFailure(Jedis store, String methodKey, Throwable t) {
            LOGGER.error("Hiba a metodus feldolgozasa kozben!", t);
            final Transaction tx = store.multi();
            tx.hset(methodKey, DataHandling.MethodResponseKeys.STATUS.name(), "ERROR");
            tx.hset(methodKey, DataHandling.MethodResponseKeys.EXCEPTION_CLASS.name(), t.getClass().getName());
            tx.hset(methodKey, DataHandling.MethodResponseKeys.EXCEPTION_MESSAGE.name(), t.getMessage());
            tx.exec();
        }
    }

    /**
     * Elindit egy esemeny feldolgozast, ha egyelore nincsen tobb feldolgozo, mint MAX_EVENTS.
     *
     * @param eventUuid - uuid of event to process
     * @throws IllegalArgumentException   if eventUuid is null
     * @throws RejectedExecutionException ha az executor nem tudja megenni az esemenyt
     */
    private void startEventProcessing(UUID eventUuid) {
        if (eventUuid == null) {
            throw new IllegalArgumentException("can not start processing event will null uuid!");
        } else if (processingEvents.incrementAndGet() < MAX_EVENTS) {
            try {
                consumerConfig.getExecutor().execute(new TrampolineRunner(new EventRunnable(eventUuid)));
            } catch (RejectedExecutionException e) {
                LOGGER.debug("Could not execute event of id {}", eventUuid);
                processingEvents.decrementAndGet();
                throw e;
            }
        } else {
            LOGGER.debug("Can not start executing event of id {} - queue is full.", eventUuid);
            processingEvents.decrementAndGet();
        }
    }

    // csokkenti a folyamatban levo eventek szamat.
    private final class EventRunnable implements Trampoline {
        private final UUID eventUuid;

        EventRunnable(UUID uuid) {
            this.eventUuid = uuid;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Trampoline jump() {
            try {
                final DefaultCallback callback = new DefaultCallback(consumerConfig.getPool(), eventUuid, getKeyNaming(), getConsumerIdentity());

                final EventFrame frame;
                try (final Jedis store = consumerConfig.getPool().getResource()) {

                    final String groupCode = getConsumerIdentity().getEventGroup();
                    // megprobaljuk ellopni

                    long result = store.hsetnx(getKeyNaming().eventDetailsKey(groupCode, eventUuid), "owner", getConsumerIdentity().getIdentifier());

                    if (result == 0) {
                        LOGGER.trace("Some other instance of {} has already gathered evt {}", groupCode, eventUuid);
                        return new AfterEventRunnable();
                    } else {
                        // itt a versenyhelyzet elkerulese miatt remove van. ha ezt kiszedjuk, megnonek a logok.
                        store.zrem(getKeyNaming().unprocessedEventsByGroupKey(groupCode), eventUuid.toString());
                        frame = DataHandling.readEventFrame(store, getKeyNaming(), consumerHandlers.getEventResolver(), eventUuid);
                    }
                } catch (DeserializationException e) {
                    callback.error(e);
                    return new AfterEventRunnable();
                }

                callback.markProcessing();
                LOGGER.trace("Elkezdtem dolgozni az esmenyen! {}", eventUuid);
                getEventProcessorAdapter().handle(frame.getEventMarker(), frame.getMetaData(), frame.getEventData(), callback);

                consumerConfig.getExecutor().execute(new TrampolineRunner(new AfterEventRunnable()));

                // TODO: itt neki kellene allni feldolgozni mas, beragadt esemenyeket is.
            } finally {
                processingEvents.decrementAndGet();
            }
            return new AfterEventRunnable();
        }
    }

    interface Trampoline {
        Trampoline jump();
    }

    // kap egy trampoline peldanyt es megprobalja megfuttatni.
    private final class TrampolineRunner implements Runnable {
        private Trampoline trampoline;

        private TrampolineRunner(Trampoline t) {
            trampoline = t;
        }

        @Override
        public void run() {
            while (trampoline != null) {
                trampoline = trampoline.jump();
            }
        }
    }

    /**
     * Run it after every events.
     * Looks for events in the pool. Tries to
     */
    private final class AfterEventRunnable implements Trampoline {

        @Override
        public Trampoline jump() {
            final String groupCode = getConsumerIdentity().getEventGroup();
            final String dbKey = getKeyNaming().unprocessedEventsByGroupKey(groupCode);

            try (final Jedis store = consumerConfig.getPool().getResource()) {
                final Set<String> elems = store.zrangeByScore(dbKey, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 1);

                if (elems != null && !elems.isEmpty()) {
                    final UUID eventUuid = UUID.fromString(elems.iterator().next());
                    return new EventRunnable(eventUuid);
                }
            } catch (Throwable t) {
                LOGGER.error("Error on automatic event processing: ", t);
            }
            return null;
        }
    }

    public void addMethodReceivingCallback(MethodReceivingCallback listener) {
        if (listener == null) {
            throw new IllegalArgumentException("Method evt listener must not be null!");
        } else {
            methodEventListeners.add(listener);
        }
    }

    public void removeMethodReceivingCallback(MethodReceivingCallback listener) {
        if (listener == null) {
            throw new IllegalArgumentException("Method sending event listener must not be null!");
        } else {
            methodEventListeners.remove(listener);
        }
    }
}
