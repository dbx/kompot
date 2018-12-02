package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.Consumer;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventReceivingCallback;
import hu.dbx.kompot.consumer.async.handler.EventProcessorAdapter;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastProcessorFactory;
import hu.dbx.kompot.consumer.broadcast.handler.SelfDescribingBroadcastProcessor;
import hu.dbx.kompot.consumer.sync.MethodReceivingCallback;
import hu.dbx.kompot.consumer.sync.handler.DefaultMethodProcessorAdapter;
import hu.dbx.kompot.core.KeyNaming;
import hu.dbx.kompot.core.SerializeHelper;
import hu.dbx.kompot.exceptions.DeserializationException;
import hu.dbx.kompot.impl.consumer.ConsumerConfig;
import hu.dbx.kompot.impl.consumer.ConsumerHandlers;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
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
     * <p>
     * We use this counter to separate the thread pool to two parts:
     * one for event handling and one for methods + broadcasts.
     */
    private final AtomicInteger processingEvents = new AtomicInteger(0);

    private final ConsumerHandlers consumerHandlers;
    private final ConsumerConfig consumerConfig;

    // TODO: maybe put these two into ConsumerHandlers?
    private final List<MethodReceivingCallback> methodEventListeners = new CopyOnWriteArrayList<>();
    private final List<EventReceivingCallback> eventReceivingCallbacks = new CopyOnWriteArrayList<>();

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
                    MethodRunnable runnable = new MethodRunnable(ConsumerImpl.this, consumerConfig, methodEventListeners, consumerHandlers, UUID.fromString(message));
                    consumerConfig.getExecutor().execute(runnable);
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
        LOGGER.debug("started daemon thread.");
        consumerConfig.getExecutor().execute(new TrampolineRunner(new AfterEventRunnable(this, consumerConfig, processingEvents, consumerHandlers, eventReceivingCallbacks)));
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

        // nekem cimzett esemenyek
        channels.add("e:" + getConsumerIdentity().getEventGroup());

        // nekem cimzett metodusok
        channels.add("m:" + getConsumerIdentity().getMessageGroup());

        // tamogatott broadcast uzenet tipusok
        consumerHandlers.getBroadcastProcessorFactory().getSupportedBroadcasts().forEach(b -> channels.add("b:" + b.getBroadcastCode()));

        // szemelyesen nekem cimzett visszajelzesek
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
            } catch (Throwable t) {
                LOGGER.error("Error handling broadcast code=" + broadcastCode + " data=" + data, t);
            }
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
                consumerConfig.getExecutor().execute(new TrampolineRunner(new EventRunnable(this, consumerConfig, processingEvents, consumerHandlers, eventUuid, eventReceivingCallbacks)));
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

    interface Trampoline {
        Trampoline jump();
    }

    // kap egy trampoline peldanyt es megprobalja megfuttatni.
    public final static class TrampolineRunner implements Runnable {
        private Trampoline trampoline;

        public TrampolineRunner(Trampoline t) {
            trampoline = t;
        }

        @Override
        public void run() {
            try {
                while (trampoline != null) {
                    trampoline = trampoline.jump();
                }
            } catch (Throwable t) {
                LOGGER.error("Error on trampoline=" + trampoline, t);
            }
        }
    }

    public void addMethodReceivingCallback(MethodReceivingCallback callback) {
        if (callback == null) {
            throw new IllegalArgumentException("Method evt callback must not be null!");
        } else {
            methodEventListeners.add(callback);
        }
    }

    // TODO: write tests
    public void removeMethodReceivingCallback(MethodReceivingCallback listener) {
        if (listener == null) {
            throw new IllegalArgumentException("Method sending event listener must not be null!");
        } else {
            methodEventListeners.remove(listener);
        }
    }

    public void addEventReceivingCallback(EventReceivingCallback callback) {
        if (callback == null) {
            throw new IllegalArgumentException("Event receiving callback must not be null!");
        } else {
            eventReceivingCallbacks.add(callback);
        }
    }

    public void removeEventReceivingCallback(EventReceivingCallback callback) {
        if (callback == null) {
            throw new IllegalArgumentException("Event receiving callback must not be null!");
        } else {
            eventReceivingCallbacks.remove(callback);
        }
    }
}
