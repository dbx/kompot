package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.Consumer;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.Listener;
import hu.dbx.kompot.consumer.MessageResult;
import hu.dbx.kompot.consumer.async.EventReceivingCallback;
import hu.dbx.kompot.consumer.async.handler.EventProcessorAdapter;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastProcessorFactory;
import hu.dbx.kompot.consumer.sync.MethodReceivingCallback;
import hu.dbx.kompot.consumer.sync.handler.DefaultMethodProcessorAdapter;
import hu.dbx.kompot.impl.consumer.ConsumerConfig;
import hu.dbx.kompot.impl.consumer.ConsumerHandlers;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public final class ConsumerImpl implements Consumer, Listener {

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

    private final Map<UUID, java.util.function.Consumer<Object>> futures = new ConcurrentHashMap<>();

    /**
     * Felregisztral egy future peldanyt es var a valaszra.
     * TODO: timeout parameter is legyen!
     */
    void registerMessageFuture(UUID messageUuid, java.util.function.Consumer<Object> consumer) {
        futures.put(messageUuid, consumer);
    }

    private final CountDownLatch startLatch = new CountDownLatch(1);
    private final CountDownLatch stopLatch = new CountDownLatch(1);

    @Override
    public void afterStarted() {
        startLatch.countDown();
    }

    @Override
    public void afterStopped() {
        stopLatch.countDown();
    }

    @Override
    public MessageResult onMessage(String channel, Object message) {
        final UUID messageUuid = consumerConfig.getMessagingService().getMessageUuid(message);
        // itt ki kell talalni h kinek adom tovabb.

        if (channel.startsWith("b:")) {
            final String broadcastCode = channel.substring(2);
            LOGGER.debug("Received Broadcast of code {} for {}", broadcastCode, consumerConfig.getConsumerIdentity().getIdentifier());
            submitToExecutor(new BroadcastRunnable(broadcastCode, (String) message, consumerHandlers));
        } else if (channel.startsWith("e:")) {
            LOGGER.debug("Received event {} on channel {}, trying to start event.", messageUuid, channel);
            return startEventProcessing(message);
        } else if (channel.startsWith("m:")) {                 // uzenet keres
            LOGGER.debug("Received method {} on channel {}, trying to start method.", messageUuid, channel);
            try {
                MethodRunnable runnable = new MethodRunnable(this, methodEventListeners, consumerHandlers, message);
                submitToExecutor(runnable);
            } catch (RejectedExecutionException rejected) {
                LOGGER.error("Could not start execution, executor service rejected. maybe too much?");
            }
        } else if (channel.startsWith("id:") && futures.containsKey(messageUuid)) {
            // TODO: a response mar nem ide jon!
            LOGGER.debug("Receiving method response: {} => {}", channel, messageUuid);
            futures.remove(messageUuid).accept(message);
        } else {
            LOGGER.error("Unexpected message on channel {} => {}", channel, messageUuid);
        }

        return MessageResult.PROCESSING;
    }

    public void startDaemonThread() throws InterruptedException {
        final Set<String> supportedBroadcastCodes = consumerHandlers.getBroadcastProcessorFactory().getSupportedBroadcasts()
                .stream().map(BroadcastDescriptor::getBroadcastCode).collect(Collectors.toSet());
        getConsumerConfig().getMessagingService().start(this, supportedBroadcastCodes);

        startLatch.await();

        // we start processing earlier events.
        LOGGER.trace("started daemon thread.");

        getConsumerConfig().getMessagingService().afterStarted(this, processingEvents, consumerHandlers, eventReceivingCallbacks);
    }

    public void shutdown() {
        try {
            getConsumerConfig().getMessagingService().stop();
            stopLatch.await();
            getConsumerConfig().getMessagingService().afterStopped(consumerConfig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ConsumerIdentity getConsumerIdentity() {
        return consumerConfig.getConsumerIdentity();
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
     * Elindit egy esemeny feldolgozast, ha egyelore nincsen tobb feldolgozo, mint MAX_EVENTS.
     *
     * @param message - message of event to process
     * @throws IllegalArgumentException   if eventUuid is null
     * @throws RejectedExecutionException ha az executor nem tudja megenni az esemenyt
     */
    private MessageResult startEventProcessing(Object message) {
        final UUID messageUuid = consumerConfig.getMessagingService().getMessageUuid(message);

        if (message == null) {
            throw new IllegalArgumentException("can not start processing event will null uuid!");
        } else if (processingEvents.incrementAndGet() <= consumerConfig.getMaxEventThreadCount()) {
            try {
                submitToExecutor(new EventRunnable(this, processingEvents, consumerHandlers, message, eventReceivingCallbacks));
            } catch (RejectedExecutionException e) {
                LOGGER.debug("Could not execute event of message {}", messageUuid);
                processingEvents.decrementAndGet();
                throw e;
            }
        } else {
            LOGGER.debug("Can not start executing event of message {} - max event thread count reached.", messageUuid);
            processingEvents.decrementAndGet();
            return MessageResult.REJECTED;
        }

        return MessageResult.PROCESSING;
    }

    private void submitToExecutor(Runnable runnable) {
        consumerConfig.getExecutor().execute(runnable);
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
