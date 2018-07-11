package hu.dbx.kompot.ng.metadata;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.TestRedis;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.handler.SelfDescribingEventProcessor;
import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.consumer.sync.handler.SelfDescribingMethodProcessor;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.LoggerUtils;
import hu.dbx.kompot.moby.MetaDataHolder;
import hu.dbx.kompot.producer.EventGroupProvider;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MetadataSendingTest {

    private static final Logger LOGGER = LoggerUtils.getLogger();

    private static final EventDescriptor<Map> EVENT_1 = EventDescriptor.of("CONSUMER", Map.class);
    private static final MethodDescriptor<Map, Map> METHOD_1 = MethodDescriptor.ofName("CONSUMER", "method1");
    private static final ConsumerIdentity consumerIdentity = groupGroup("CONSUMER");
    private static final ConsumerIdentity producerIdentity = groupGroup("PRODUCER");

    @Rule
    public TestRedis redis = TestRedis.build();

    @Test
    public void testEventMetadataSending() throws InterruptedException, SerializationException {
        final ExecutorService executor = Executors.newFixedThreadPool(4);

        AtomicReference<MetaDataHolder> outputMeta = new AtomicReference<>();

        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.empty(), consumerIdentity, executor);
        consumer.registerEventHandler(SelfDescribingEventProcessor.of(EVENT_1, (data, meta) -> {
            LOGGER.info("Test Callback Processed");

            outputMeta.set(meta);

        }));
        consumer.start();

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);
        producer.start();

        producer.asyncSendEvent(EVENT_1, singletonMap("aa", 0), MetaDataHolder.build("corri", "usrR"));

        Thread.sleep(1000);

        assertNotNull(outputMeta.get());
        assertEquals("corri", outputMeta.get().getCorrelationId());
        assertEquals("usrR", outputMeta.get().getUserRef());

        producer.stop();
        consumer.stop();
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

    }

    @Test
    public void testSuccessfullyHandleEvent() throws InterruptedException, SerializationException, ExecutionException, TimeoutException {
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        AtomicReference<MetaDataHolder> outputMeta = new AtomicReference<>();

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);

        producer.start();

        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), consumerIdentity, executor);

        consumer.registerMethodProcessor(SelfDescribingMethodProcessor.of(METHOD_1, (x, meta) -> {
            LOGGER.info("Processing " + x);

            outputMeta.set(meta);

            return singletonMap("a", 1);
        }));

        consumer.start();

        Thread.sleep(1000);
        CompletableFuture<Map> response = producer.syncCallMethod(METHOD_1.withTimeout(100_000), singletonMap("aa", 11), MetaDataHolder.build("xxx", "yyy"));

        assertEquals(1, response.get(3, TimeUnit.SECONDS).get("a"));

        assertNotNull(outputMeta.get());
        assertEquals("xxx", outputMeta.get().getCorrelationId());
        assertEquals("yyy", outputMeta.get().getUserRef());


        producer.stop();
        consumer.stop();
        executor.shutdown();
    }

}
