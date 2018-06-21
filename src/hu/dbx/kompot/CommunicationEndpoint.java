package hu.dbx.kompot;

import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.handler.DefaultEventProcessorAdapter;
import hu.dbx.kompot.consumer.async.handler.EventProcessorFactory;
import hu.dbx.kompot.consumer.async.handler.SelfDescribingEventProcessor;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.broadcast.handler.DefaultBroadcastProcessorAdapter;
import hu.dbx.kompot.consumer.broadcast.handler.SelfDescribingBroadcastProcessor;
import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.consumer.sync.MethodSendingEventListener;
import hu.dbx.kompot.consumer.sync.handler.DefaultMethodProcessorAdapter;
import hu.dbx.kompot.consumer.sync.handler.SelfDescribingMethodProcessor;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.ConsumerImpl;
import hu.dbx.kompot.impl.DefaultKeyNaming;
import hu.dbx.kompot.impl.ProducerImpl;
import hu.dbx.kompot.impl.consumer.ConsumerConfig;
import hu.dbx.kompot.impl.consumer.ConsumerHandlers;
import hu.dbx.kompot.producer.EventGroupProvider;
import hu.dbx.kompot.producer.ProducerIdentity;
import redis.clients.jedis.JedisPool;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SuppressWarnings("WeakerAccess")
public final class CommunicationEndpoint {

    public static final int DEFAULT_EXECUTOR_THREADS = 12;

    private final DefaultEventProcessorAdapter events = new DefaultEventProcessorAdapter();
    private final DefaultMethodProcessorAdapter methods = new DefaultMethodProcessorAdapter();
    private final DefaultBroadcastProcessorAdapter broadcasts = new DefaultBroadcastProcessorAdapter();

    private final ProducerImpl producer;
    private final ConsumerImpl consumer;

    private static final DefaultKeyNaming naming = DefaultKeyNaming.ofPrefix("moby");

    public static CommunicationEndpoint ofRedisConnectionUri(URI connection, EventGroupProvider groups, ConsumerIdentity serverIdentity) {
        return ofRedisConnectionUri(connection, groups, serverIdentity, Executors.newFixedThreadPool(DEFAULT_EXECUTOR_THREADS));
    }

    public static CommunicationEndpoint ofRedisConnectionUri(URI connection, EventGroupProvider groups, ConsumerIdentity serverIdentity, ExecutorService executor) {
        return new CommunicationEndpoint(new JedisPool(connection), groups, serverIdentity, ProducerIdentity.randomUuidIdentity(), executor);
    }

    private CommunicationEndpoint(JedisPool pool, EventGroupProvider groups, ConsumerIdentity serverIdentity, ProducerIdentity producerIdentity, ExecutorService executor) {
        final ConsumerConfig config = new ConsumerConfig(executor, serverIdentity, pool, naming);
        final ConsumerHandlers handlers = new ConsumerHandlers(events, events, broadcasts, broadcasts, methods, methods);

        this.consumer = new ConsumerImpl(config, handlers);
        this.producer = new ProducerImpl(pool, groups, naming, this.consumer, producerIdentity);
    }

    /**
     * Elinditja a komponenst.
     *
     * @throws IllegalStateException ha mar elindult korabban
     */
    public void start() throws IllegalStateException {
        try {
            consumer.startDaemonThread();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Leallitja a komponenst.
     *
     * @throws IllegalStateException ha meg nem indult el a komponens
     */
    public void stop() throws IllegalStateException {

        consumer.shutdown();
        producer.shutdown();
        // throw new IllegalStateException("Nincs implementalva!");
    }

    public boolean isRunning() {
        throw new IllegalStateException("Nincs implementalva!");
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

    public <TReq> void asyncSendEventOnSuccessfulTransaction(EventDescriptor<TReq> event, TReq data) {
        // felregisztral a tranzakciora, csak siker eseten kuld esemenyt!
    }

    /**
     * Hiv egy szinkron uzenetet es megvarja a valaszt.
     *
     * @param method    a tavoli metodus amit hivunk
     * @param data      a keres adattartama
     * @param <TReq>    a keres tipusa
     * @param <TRes>    a valasz tipusa
     * @return a hivas eredmenyt becsomagolva
     * @throws SerializationException ha nem tudtuk a peldanyt szerializalni
     * @throws IllegalStateException  ha nem fut a komponens
     */
    public <TReq, TRes> CompletableFuture<TRes> syncCallMethod(MethodDescriptor<TReq, TRes> method, TReq data) throws SerializationException, IllegalStateException {
        return producer.sendMessage(method, data);
    }

    /**
     * Felregisztral egy esemenykezelot
     */
    public void registerMethodSendingEventListener(MethodSendingEventListener eventListener) {
        throw new RuntimeException("Not implemented!");
    }

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
        broadcasts.register(broadcastProcessor);
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
}
