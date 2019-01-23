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
import hu.dbx.kompot.core.ThreadSafePubSub;
import hu.dbx.kompot.exceptions.DeserializationException;
import hu.dbx.kompot.impl.consumer.ConsumerConfig;
import hu.dbx.kompot.impl.consumer.ConsumerHandlers;
import hu.dbx.kompot.status.SelfStatusWriter;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public final class ConsumerImpl implements Consumer, ThreadSafePubSub.Listener {

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
        this.pubSub = new ThreadSafePubSub(consumerConfig.getPool(), this);
    }

    private final Map<UUID, Runnable> futures = new ConcurrentHashMap<>();

    private final ThreadSafePubSub pubSub;

    /**
     * Felregisztral egy future peldanyt es var a valaszra.
     * TODO: timeout parameter is legyen!
     */
    void registerMessageFuture(UUID messageUuid, Runnable runnable) {
        futures.put(messageUuid, runnable);
    }

    private final CountDownLatch startLatch = new CountDownLatch(1);
    private final CountDownLatch stopLatch = new CountDownLatch(1);


    @Override
    public void onSubscribe(String channel, int subscribedChannels) {
        if (subscribedChannels == getPubSubChannels().length) {
            startLatch.countDown();
        }
    }

    @Override
    public void onUnsubscribe(String channel, int subscribedChannels) {
        if (0 == subscribedChannels) {
            stopLatch.countDown();
        }
    }

    @Override
    public void onMessage(String channel, String message) {
        // itt ki kell talalni h kinek adom tovabb.
        if (channel.startsWith("b:")) {
            final String broadcastCode = channel.substring(2);
            LOGGER.info("Received Broadcast of code {} for {}", broadcastCode, consumerConfig.getConsumerIdentity().getIdentifier());
            submitToExecutor(new BroadcastRunnable(broadcastCode, message));
        } else if (channel.startsWith("e:")) {
            startEventProcessing(UUID.fromString(message));
        } else if (channel.startsWith("m:")) {                 // uzenet keres
            LOGGER.debug("Received message bang {} on channel {}, trying to start method.", message, channel);
            //noinspection unused
            final String methodName = channel.substring(2);
            try {
                MethodRunnable runnable = new MethodRunnable(ConsumerImpl.this, consumerConfig, methodEventListeners, consumerHandlers, UUID.fromString(message));
                submitToExecutor(runnable);
            } catch (RejectedExecutionException rejected) {
                LOGGER.error("Could not start execution, executor service rejected. maybe too much?");
            }
        } else if (channel.startsWith("id:") && futures.containsKey(UUID.fromString(message))) {
            // TODO: a response mar nem ide jon!
            LOGGER.debug("Receiving method response: {} => {}", channel, message);
            futures.remove(UUID.fromString(message)).run();
        } else {
            LOGGER.error("Unexpected message on channel {} => {}", channel, message);
        }
    }

    // TODO: how to stop it when undeployed?
    // TODO: do not use threading here.
    public void startDaemonThread() throws InterruptedException {
        pubSub.startWithChannels(getPubSubChannels());

        startLatch.await();

        // we start processing earlier events.
        LOGGER.debug("started daemon thread.");
        // csak azert noveljuk, mert az after event runnable csokkenteni fogja!
        processingEvents.incrementAndGet();

        // ez fogja a modul statuszat rendszeresen beleirni.
        SelfStatusWriter.start(consumerConfig);

        submitToExecutorTrampoline(new AfterEventRunnable(this, consumerConfig, processingEvents, consumerHandlers, eventReceivingCallbacks));
    }

    public void shutdown() {
        try {
            pubSub.unsubscrubeAllAndStop();
            stopLatch.await();

            SelfStatusWriter.delete(consumerConfig);

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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
                submitToExecutorTrampoline(new EventRunnable(this, consumerConfig, processingEvents, consumerHandlers, eventUuid, eventReceivingCallbacks));
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

    private void submitToExecutorTrampoline(Trampoline trampoline) {
        submitToExecutor(new TrampolineRunner(trampoline));
    }

    private void submitToExecutor(Runnable runnable) {
        consumerConfig.getExecutor().execute(runnable);
    }

    interface Trampoline {
        Trampoline jump();
    }

    // kap egy trampoline peldanyt es megprobalja megfuttatni.
    public final static class TrampolineRunner implements Runnable {
        private Trampoline trampoline;

        TrampolineRunner(Trampoline t) {
            trampoline = t;
        }

        @Override
        public void run() {
            try {
                LOGGER.info("Started trampoline.");
                while (trampoline != null) {
                    trampoline = trampoline.jump();
                }
                LOGGER.info("Exited trampoline.");
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

    public ConsumerConfig getConsumerConfig() {
        return consumerConfig;
    }
}
