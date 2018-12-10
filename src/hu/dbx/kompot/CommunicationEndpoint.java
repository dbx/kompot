package hu.dbx.kompot;

import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.EventReceivingCallback;
import hu.dbx.kompot.consumer.async.EventSendingCallback;
import hu.dbx.kompot.consumer.async.handler.DefaultEventProcessorAdapter;
import hu.dbx.kompot.consumer.async.handler.EventProcessorFactory;
import hu.dbx.kompot.consumer.async.handler.SelfDescribingEventProcessor;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.broadcast.handler.DefaultBroadcastProcessorAdapter;
import hu.dbx.kompot.consumer.broadcast.handler.SelfDescribingBroadcastProcessor;
import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.consumer.sync.MethodReceivingCallback;
import hu.dbx.kompot.consumer.sync.MethodSendingCallback;
import hu.dbx.kompot.consumer.sync.handler.DefaultMethodProcessorAdapter;
import hu.dbx.kompot.consumer.sync.handler.SelfDescribingMethodProcessor;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.ConsumerImpl;
import hu.dbx.kompot.impl.DefaultKeyNaming;
import hu.dbx.kompot.impl.LoggerUtils;
import hu.dbx.kompot.impl.ProducerImpl;
import hu.dbx.kompot.impl.consumer.ConsumerConfig;
import hu.dbx.kompot.impl.consumer.ConsumerHandlers;
import hu.dbx.kompot.impl.producer.ProducerConfig;
import hu.dbx.kompot.moby.MetaDataHolder;
import hu.dbx.kompot.producer.EventGroupProvider;
import hu.dbx.kompot.producer.ProducerIdentity;
import hu.dbx.kompot.status.StatusItemImpl;
import hu.dbx.kompot.status.StatusReport;
import hu.dbx.kompot.status.StatusReporter;
import hu.dbx.kompot.status.StatusRequestBroadcastHandler;
import org.slf4j.Logger;
import redis.clients.jedis.JedisPool;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Use this component as a simplified interface to access library functionality.
 */
@SuppressWarnings("WeakerAccess")
public final class CommunicationEndpoint {
    private static final Logger LOGGER = LoggerUtils.getLogger();

    public static final int DEFAULT_EXECUTOR_THREADS = 12;

    private final DefaultEventProcessorAdapter events = new DefaultEventProcessorAdapter();
    private final DefaultMethodProcessorAdapter methods = new DefaultMethodProcessorAdapter();
    private final DefaultBroadcastProcessorAdapter broadcasts = new DefaultBroadcastProcessorAdapter();

    private final ProducerImpl producer;
    private final ConsumerImpl consumer;

    private final AtomicBoolean starting = new AtomicBoolean(false);
    private final AtomicBoolean started = new AtomicBoolean(false);


    private final List<StatusReporter> statusReporters = new ArrayList<>();

    // TODO: make prefix configurable!
    private static final DefaultKeyNaming naming = DefaultKeyNaming.ofPrefix("moby");

    /**
     * Constructs a new instance with a default executor service.
     */
    public static CommunicationEndpoint ofRedisConnectionUri(URI connection, EventGroupProvider groups, ConsumerIdentity serverIdentity) {
        return ofRedisConnectionUri(connection, groups, serverIdentity, Executors.newFixedThreadPool(DEFAULT_EXECUTOR_THREADS));
    }

    /**
     * Constructs a new instance with a custom executor serviec.
     */
    public static CommunicationEndpoint ofRedisConnectionUri(URI connection, EventGroupProvider groups, ConsumerIdentity serverIdentity, ExecutorService executor) {
        return new CommunicationEndpoint(new JedisPool(connection), groups, serverIdentity, ProducerIdentity.randomUuidIdentity(), executor);
    }

    private CommunicationEndpoint(JedisPool pool, EventGroupProvider groups, ConsumerIdentity serverIdentity, ProducerIdentity producerIdentity, ExecutorService executor) {

        final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        final ConsumerConfig consumerConfig = new ConsumerConfig(executor, scheduledExecutor, serverIdentity, pool, naming);
        final ConsumerHandlers handlers = new ConsumerHandlers(events, events, broadcasts, broadcasts, methods, methods);

        final ProducerConfig producerConfig = new ProducerConfig(executor, scheduledExecutor, pool, naming, producerIdentity);

        this.consumer = new ConsumerImpl(consumerConfig, handlers);
        this.producer = new ProducerImpl(producerConfig, groups, this.consumer);

        registerBroadcastProcessor(new StatusRequestBroadcastHandler(this::findLocalStatusReport, consumerConfig));
    }

    /**
     * Starts component or throws.
     *
     * @throws IllegalStateException When already started.
     */
    public void start() throws IllegalStateException {
        if (starting.compareAndSet(false, true)) {
            try {
                consumer.startDaemonThread();
                started.set(true);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new IllegalStateException("Method start() has already been called.");
        }
    }

    /**
     * Stops component or throws.
     *
     * @throws IllegalStateException if component is not running already.
     */
    public void stop() throws IllegalStateException {
        if (started.compareAndSet(true, false)) {
            consumer.shutdown();
            producer.shutdown();
            // throw new IllegalStateException("Nincs implementalva!");
            starting.set(false);
        } else {
            throw new IllegalStateException("Method start() has not yet been called.");
        }
    }

    /**
     * Returns true iff component is up and running.
     */
    public boolean isRunning() {
        return started.get();
    }

    /**
     * Hiv egy aszinkron esemenyt. Eljuttatja a feliratkozottakhoz.
     *
     * @param event  esemeny leiroja
     * @param data   esemeny adattartama
     * @param <TReq> keres adattartam tipusa
     * @throws SerializationException ha a keresi adat nem szerializalhato
     * @throws IllegalStateException  ha eppen nem fut a komponensunk
     */
    public <TReq> void asyncSendEvent(EventDescriptor<TReq> event, TReq data) throws SerializationException, IllegalStateException {
        producer.sendEvent(event, data);
    }

    public <TReq> void asyncSendEvent(EventDescriptor<TReq> event, TReq data, MetaDataHolder metaData) throws SerializationException, IllegalStateException {
        producer.sendEvent(event, data, metaData);
    }

    /**
     * Calls a synchronous methods and returns a future representing the result.
     *
     * @param method Remote method descriptor, must not be null.
     * @param data   request data payload. Must be able to serialize.
     * @param <TReq> type of request
     * @param <TRes> type of response
     * @return future representing call result data.
     * @throws SerializationException when could not serialize data payload.
     * @throws IllegalStateException  if current component is not running.
     * @throws NullPointerException   when method argument is null.
     */
    public <TReq, TRes> CompletableFuture<TRes> syncCallMethod(MethodDescriptor<TReq, TRes> method, TReq data) throws SerializationException, IllegalStateException {
        return producer.sendMessage(method, data);
    }

    public <TReq, TRes> CompletableFuture<TRes> syncCallMethod(MethodDescriptor<TReq, TRes> method, TReq data, MetaDataHolder metaDataHolder) throws SerializationException, IllegalStateException {
        return producer.sendMessage(method, data, metaDataHolder);
    }


    /**
     * Registers callback that is called to follow up on the lifecycle of sending a method call.
     *
     * @param eventListener not null callback object
     * @throws IllegalArgumentException on null parameter
     */
    public void registerMethodSendingCallback(MethodSendingCallback eventListener) throws IllegalArgumentException {
        producer.addMethodSendingCallback(eventListener);
    }

    /**
     * Registers a callback that is called to follow up on the lifecycle of receiving and processing a method call.
     *
     * @param eventListener not null callback object
     * @throws IllegalArgumentException on null parameter
     */
    public void registerMethodReceivingCallback(MethodReceivingCallback eventListener) throws IllegalArgumentException {
        consumer.addMethodReceivingCallback(eventListener);
    }

    /**
     * Registers a callback that is called to follow up on the lifecycle of receiving and processing an event call.
     * <p>
     * Such callbacks can be used to set up a local processing context, logging, etc.
     *
     * @param callback not null callback object
     * @throws IllegalArgumentException on null parameter
     */
    public void registerEventReceivingCallback(EventReceivingCallback callback) throws IllegalArgumentException {
        consumer.addEventReceivingCallback(callback);
    }

    /**
     * Registers callback that is called to follow up on the lifecycle of sending an event call.
     *
     * @param eventListener not null callback object
     * @throws IllegalArgumentException on null parameter
     */
    public void registerEventSendingCallback(EventSendingCallback eventListener) throws IllegalArgumentException {
        producer.addEventSendingCallback(eventListener);
    }

    /**
     * Registers a factory object that is used to generate event processors.
     *
     * @param factory not null.
     */
    public void registerEventHandlers(EventProcessorFactory factory) {
        events.register(factory);
    }

    public void registerEventHandler(SelfDescribingEventProcessor adapter) {
        events.register(adapter);
    }

    public void registerMethodProcessor(SelfDescribingMethodProcessor methodProcessor) {
        methods.register(methodProcessor);
    }

    /**
     * Felregisztral egy esemenykezelot arra az esetre, ha broadcast uzenetet kapunk
     */
    public void registerBroadcastProcessor(SelfDescribingBroadcastProcessor broadcastProcessor) {
        if (starting.get()) {
            throw new IllegalStateException("Process has already been started!");
        } else {
            broadcasts.register(broadcastProcessor);
        }
    }

    /**
     * Kikuld egy broadcast esemenyt. Csak szerializacios hiba eseten dob.
     *
     * @throws SerializationException ha a data nem szerializalhato.
     * @throws IllegalStateException  ha eppen nem fut a komponens.
     */
    public <TReq> void broadcast(BroadcastDescriptor<TReq> descriptor, TReq data) throws SerializationException, IllegalStateException {
        producer.broadcast(descriptor, data);
    }

    /**
     * Adds a new system status reporting
     */
    public void registerStatusReporter(StatusReporter reporter) {
        if (reporter == null) {
            throw new IllegalArgumentException("Reporter should not be null!");
        }
        statusReporters.add(reporter);
    }

    /**
     * Returns a list of all systems found in the current component.
     */
    private List<StatusReport.StatusItem> findLocalStatuses() {

        List<StatusReport.StatusItem> result = new ArrayList<>();

        for (StatusReporter statusReporter : statusReporters) {
            StatusReporter.StatusResult statusResult;
            try {
                statusResult = statusReporter.getEndpoint().call();
            } catch (Exception e) {
                LOGGER.error("Error caught while calling statusReporter " + statusReporter, e);
                statusResult = StatusReporter.StatusResult.resultError("Endpoint ");
            }
            result.add(new StatusItemImpl(statusReporter.getName(), statusReporter.getDescription(), statusResult.getErrorMessage()));
        }
        return result;
    }

    public StatusReport findLocalStatusReport() {
        final List<StatusReport.StatusItem> items = findLocalStatuses();
        final ConsumerIdentity id = consumer.getConsumerIdentity();

        Set<String> methods = null;
        Set<String> events = null;
        Set<String> broadcasts = null;

        return new StatusReport(id, null, items, methods, events, broadcasts);
    }

    public List<StatusReport.StatusItem> findGlobalStatuses() {

        // 0. megszamolom, h hanyan vannak a halozaton
        // 1. broadcast kikuldese random kulcsnevvel
        // 2. beolvasni az n statusz teteleket a kulcs alol.
        return null;
    }
}
