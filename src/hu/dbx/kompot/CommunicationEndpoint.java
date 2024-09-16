package hu.dbx.kompot;

import com.rabbitmq.client.ConnectionFactory;
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
import hu.dbx.kompot.core.MessagingService;
import hu.dbx.kompot.core.StatusReportingAction;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.BlockingLifecycle;
import hu.dbx.kompot.impl.ConsumerImpl;
import hu.dbx.kompot.impl.ProducerImpl;
import hu.dbx.kompot.impl.consumer.ConsumerConfig;
import hu.dbx.kompot.impl.consumer.ConsumerHandlers;
import hu.dbx.kompot.impl.jedis.DefaultKeyNaming;
import hu.dbx.kompot.impl.jedis.JedisMessagingService;
import hu.dbx.kompot.impl.jedis.status.JedisStatusReportingAction;
import hu.dbx.kompot.impl.producer.ProducerConfig;
import hu.dbx.kompot.impl.rabbit.RabbitMessagingService;
import hu.dbx.kompot.impl.rabbit.status.RabbitStatusReportingAction;
import hu.dbx.kompot.moby.MetaDataHolder;
import hu.dbx.kompot.producer.EventGroupProvider;
import hu.dbx.kompot.producer.ProducerIdentity;
import hu.dbx.kompot.status.StatusReport;
import hu.dbx.kompot.status.StatusReporter;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.*;

import static java.util.Collections.emptyList;

/**
 * Use this component as a simplified interface to access library functionality.
 */
@SuppressWarnings("WeakerAccess")
public final class CommunicationEndpoint {

    public static final int DEFAULT_EXECUTOR_THREADS = 12;
    public static final List<String> DEFAULT_LOG_SENSITIVE_DATA_KEYS = emptyList();
    public static final int DEFAULT_MAX_EVENT_THREAD_COUNT = 3;

    private final DefaultEventProcessorAdapter events = new DefaultEventProcessorAdapter();
    private final DefaultMethodProcessorAdapter methods = new DefaultMethodProcessorAdapter();
    private final DefaultBroadcastProcessorAdapter broadcasts = new DefaultBroadcastProcessorAdapter();

    private final ProducerImpl producer;
    private final ConsumerImpl consumer;

    private final BlockingLifecycle lifecycle = new BlockingLifecycle();

    private StatusReportingAction statusReportingAction;

    /**
     * Constructs a new instance with a default executor service.
     */
    public static CommunicationEndpoint ofRedisConnectionUri(URI connection,
                                                             EventGroupProvider groups,
                                                             ConsumerIdentity serverIdentity) {
        return ofRedisConnectionUri(connection, groups, serverIdentity,
                Executors.newFixedThreadPool(DEFAULT_EXECUTOR_THREADS));
    }

    public static CommunicationEndpoint ofRedisConnectionUri(URI connection,
                                                             EventGroupProvider groups,
                                                             ConsumerIdentity serverIdentity,
                                                             int maxEventThreadCount) {
        return ofRedisConnectionUri(connection, groups, serverIdentity,
                Executors.newFixedThreadPool(DEFAULT_EXECUTOR_THREADS), DEFAULT_LOG_SENSITIVE_DATA_KEYS, maxEventThreadCount);
    }

    @Deprecated
    public static CommunicationEndpoint ofRedisConnectionUri(URI connection,
                                                             EventGroupProvider groups,
                                                             ConsumerIdentity serverIdentity,
                                                             ExecutorService executor) {
        return ofRedisConnectionUri(connection, groups, serverIdentity, new ProducerIdentity.RandomUuidIdentity(),
                executor, DEFAULT_LOG_SENSITIVE_DATA_KEYS, DEFAULT_MAX_EVENT_THREAD_COUNT);
    }

    /**
     * Constructs a new instance with a custom executor serviec.
     */
    public static CommunicationEndpoint ofRedisConnectionUri(URI connection,
                                                             EventGroupProvider groups,
                                                             ConsumerIdentity serverIdentity,
                                                             ProducerIdentity producerIdentity,
                                                             ExecutorService executor) {
        return ofRedisConnectionUri(connection, groups, serverIdentity, producerIdentity, executor,
                DEFAULT_LOG_SENSITIVE_DATA_KEYS, DEFAULT_MAX_EVENT_THREAD_COUNT);
    }

    /**
     * Constructs a new instance with a custom executor serviece and log sensitive data keys
     */
    public static CommunicationEndpoint ofRedisConnectionUri(URI connection,
                                                             EventGroupProvider groups,
                                                             ConsumerIdentity serverIdentity,
                                                             ExecutorService executor,
                                                             List<String> logSensitiveDataKeys) {
        return ofRedisConnectionUri(connection, groups, serverIdentity, new ProducerIdentity.RandomUuidIdentity(),
                executor, logSensitiveDataKeys, DEFAULT_MAX_EVENT_THREAD_COUNT);
    }

    public static CommunicationEndpoint ofRedisConnectionUri(URI connection,
                                                             EventGroupProvider groups,
                                                             ConsumerIdentity serverIdentity,
                                                             ExecutorService executor,
                                                             List<String> logSensitiveDataKeys,
                                                             int maxEventThreadCount) {
        return ofRedisConnectionUri(connection, groups, serverIdentity, new ProducerIdentity.RandomUuidIdentity(),
                executor, logSensitiveDataKeys, maxEventThreadCount);
    }

    private static CommunicationEndpoint ofRedisConnectionUri(URI connection,
                                                              EventGroupProvider groups,
                                                              ConsumerIdentity serverIdentity,
                                                              ProducerIdentity producerIdentity,
                                                              ExecutorService executor,
                                                              List<String> logSensitiveDataKeys,
                                                              int maxEventThreadCount) {
        // TODO: make prefix configurable!
        final DefaultKeyNaming naming = DefaultKeyNaming.ofPrefix("moby");
        final CommunicationEndpoint communicationEndpoint = new CommunicationEndpoint(new JedisMessagingService(new JedisPool(connection), serverIdentity, naming),
                new JedisMessagingService(new JedisPool(connection), serverIdentity, naming), groups, serverIdentity, producerIdentity, executor, logSensitiveDataKeys, maxEventThreadCount);
        communicationEndpoint.statusReportingAction = new JedisStatusReportingAction(communicationEndpoint.consumer, communicationEndpoint);
        return communicationEndpoint;
    }

    public static CommunicationEndpoint ofRabbitConnectionUri(URI connection,
                                                              EventGroupProvider groups,
                                                              ConsumerIdentity serverIdentity,
                                                              int maxEventThreadCount) throws IOException, TimeoutException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
        final ConnectionFactory factory = createRabbitConnectionFactory(connection);
        final CommunicationEndpoint communicationEndpoint = new CommunicationEndpoint(new RabbitMessagingService(factory.newConnection(), serverIdentity),
                new RabbitMessagingService(factory.newConnection(), serverIdentity), groups, serverIdentity, new ProducerIdentity.RandomUuidIdentity(),
                Executors.newFixedThreadPool(DEFAULT_EXECUTOR_THREADS), DEFAULT_LOG_SENSITIVE_DATA_KEYS, maxEventThreadCount);
        communicationEndpoint.statusReportingAction = new RabbitStatusReportingAction(communicationEndpoint.consumer, communicationEndpoint);
        return communicationEndpoint;
    }

    public static CommunicationEndpoint ofRabbitConnectionUri(URI connection,
                                                              EventGroupProvider groups,
                                                              ConsumerIdentity serverIdentity,
                                                              ExecutorService executor,
                                                              List<String> logSensitiveDataKeys,
                                                              int maxEventThreadCount) throws IOException, TimeoutException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
        final ConnectionFactory factory = createRabbitConnectionFactory(connection);
        final CommunicationEndpoint communicationEndpoint = new CommunicationEndpoint(new RabbitMessagingService(factory.newConnection(), serverIdentity),
                new RabbitMessagingService(factory.newConnection(), serverIdentity), groups, serverIdentity, new ProducerIdentity.RandomUuidIdentity(),
                executor, logSensitiveDataKeys, maxEventThreadCount);
        communicationEndpoint.statusReportingAction = new RabbitStatusReportingAction(communicationEndpoint.consumer, communicationEndpoint);
        return communicationEndpoint;
    }

    private static ConnectionFactory createRabbitConnectionFactory(URI connection) throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(connection);
        return factory;
    }

    private CommunicationEndpoint(MessagingService consumerMessagingService,
                                  MessagingService producerMessagingService,
                                  EventGroupProvider groups,
                                  ConsumerIdentity serverIdentity,
                                  ProducerIdentity producerIdentity,
                                  ExecutorService executor,
                                  List<String> logSensitiveDataKeys,
                                  int maxEventThreadCount) {

        final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        final ConsumerConfig consumerConfig =
                new ConsumerConfig(executor, scheduledExecutor, serverIdentity, consumerMessagingService, logSensitiveDataKeys, maxEventThreadCount);
        final ConsumerHandlers handlers = new ConsumerHandlers(events, events, broadcasts, broadcasts, methods, methods);

        final ProducerConfig producerConfig =
                new ProducerConfig(executor, scheduledExecutor, producerMessagingService, producerIdentity);

        this.consumer = new ConsumerImpl(consumerConfig, handlers);
        this.producer = new ProducerImpl(producerConfig, groups, this.consumer);
    }

    /**
     * Starts component or throws.
     *
     * @throws IllegalStateException When already started.
     */
    public void start() throws IllegalStateException {
        lifecycle.starting(() -> {
            consumer.startDaemonThread();
            return null;
        });
    }

    /**
     * Stops component or throws.
     *
     * @throws IllegalStateException if component is not running already.
     */
    public void stop() throws IllegalStateException {
        lifecycle.stopping(() -> {
            consumer.shutdown();
            producer.shutdown();
        });
    }

    /**
     * Returns true iff component is up and running.
     */
    public boolean isRunning() {
        return lifecycle.isRunning();
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
        lifecycle.doWhenRunningThrows(() -> producer.sendEvent(event, data));
    }

    public <TReq> void asyncSendEvent(EventDescriptor<TReq> event, TReq data, MetaDataHolder metaData) throws SerializationException, IllegalStateException {
        lifecycle.doWhenRunningThrows(() -> producer.sendEvent(event, data, metaData));
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
        return lifecycle.doWhenRunningThrows(() -> producer.sendMessage(method, data));
    }

    public <TReq, TRes> CompletableFuture<TRes> syncCallMethod(MethodDescriptor<TReq, TRes> method, TReq data, MetaDataHolder metaDataHolder) throws SerializationException, IllegalStateException {
        return lifecycle.doWhenRunningThrows(() -> producer.sendMessage(method, data, metaDataHolder));
    }

    /**
     * Registers callback that is called to follow up on the lifecycle of sending a method call.
     *
     * @param eventListener not null callback object
     * @throws IllegalArgumentException on null parameter
     */
    public void registerMethodSendingCallback(MethodSendingCallback eventListener) throws IllegalArgumentException {
        lifecycle.doBeforeStarted(() -> producer.addMethodSendingCallback(eventListener));
    }

    /**
     * Registers a callback that is called to follow up on the lifecycle of receiving and processing a method call.
     *
     * @param eventListener not null callback object
     * @throws IllegalArgumentException on null parameter
     */
    @SuppressWarnings("unused")
    public void registerMethodReceivingCallback(MethodReceivingCallback eventListener) throws IllegalArgumentException {
        lifecycle.doBeforeStarted(() -> consumer.addMethodReceivingCallback(eventListener));
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
        lifecycle.doBeforeStarted(() -> consumer.addEventReceivingCallback(callback));
    }

    /**
     * Registers callback that is called to follow up on the lifecycle of sending an event call.
     *
     * @param eventListener not null callback object
     * @throws IllegalArgumentException on null parameter
     */
    public void registerEventSendingCallback(EventSendingCallback eventListener) throws IllegalArgumentException {
        lifecycle.doBeforeStarted(() -> producer.addEventSendingCallback(eventListener));
    }

    /**
     * Registers a factory object that is used to generate event processors.
     *
     * @param factory not null.
     */
    @SuppressWarnings("unused")
    public void registerEventHandlers(EventProcessorFactory factory) {
        lifecycle.doBeforeStarted(() -> events.register(factory));
    }

    public void registerEventHandler(@SuppressWarnings("rawtypes") SelfDescribingEventProcessor adapter) {
        lifecycle.doBeforeStarted(() -> events.register(adapter));
    }

    public void registerMethodProcessor(@SuppressWarnings("rawtypes") SelfDescribingMethodProcessor methodProcessor) {
        lifecycle.doBeforeStarted(() -> methods.register(methodProcessor));
    }

    /**
     * Felregisztral egy esemenykezelot arra az esetre, ha broadcast uzenetet kapunk
     */
    public void registerBroadcastProcessor(@SuppressWarnings("rawtypes") SelfDescribingBroadcastProcessor broadcastProcessor) {
        lifecycle.doBeforeStarted(() -> broadcasts.register(broadcastProcessor));
    }

    /**
     * Kikuld egy broadcast esemenyt. Csak szerializacios hiba eseten dob.
     *
     * @throws SerializationException ha a data nem szerializalhato.
     * @throws IllegalStateException  ha eppen nem fut a komponens.
     */
    public <TReq> void broadcast(BroadcastDescriptor<TReq> descriptor, TReq data) throws SerializationException, IllegalStateException {
        lifecycle.doWhenRunningThrows(() -> producer.broadcast(descriptor, data));
    }

    /**
     * Adds a new system status reporting.
     */
    public void registerStatusReporter(final StatusReporter reporter) {
        if (reporter == null) {
            throw new IllegalArgumentException("Reporter should not be null!");
        } else {
            statusReportingAction.registerStatusReporter(reporter);
        }
    }

    /**
     * Returns a list of reports from all modules on the network including the current module.
     */
    public List<StatusReport> findGlobalStatuses() throws IllegalStateException {
        return lifecycle.doWhenRunningGet(statusReportingAction::findGlobalStatuses);
    }
}
