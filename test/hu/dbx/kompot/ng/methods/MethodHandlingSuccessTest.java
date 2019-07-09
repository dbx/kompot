package hu.dbx.kompot.ng.methods;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.TestRedis;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.consumer.sync.handler.SelfDescribingMethodProcessor;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.LoggerUtils;
import hu.dbx.kompot.producer.EventGroupProvider;
import hu.dbx.kompot.producer.ProducerIdentity;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.*;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("unchecked")
public class MethodHandlingSuccessTest {
    private static final Logger LOGGER = LoggerUtils.getLogger();

    private static final MethodDescriptor METHOD_1 = MethodDescriptor.ofName("MOD_TARGET", "METHOD1");
    private static final ConsumerIdentity targetConsumerIdentity = groupGroup("MOD_TARGET");

    private static final ConsumerIdentity sourceConsumerIdentity = groupGroup("MOD_SOURCE");
    private static final ProducerIdentity sourceProducerIdentity = new ProducerIdentity.DetailedIdentity("MOD_SOURCE", "v1");


    @Rule
    public TestRedis redis = TestRedis.build();

    /**
     * Valaszolunk a metodus hivasra
     */
    @Test
    public void testSuccessfullyHandleMethod() throws InterruptedException, SerializationException, ExecutionException, TimeoutException {
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(
                redis.getConnectionURI(),
                EventGroupProvider.identity(),
                sourceConsumerIdentity,
                sourceProducerIdentity,
                executor);


        final MethodLifecycleTester tester = new MethodLifecycleTester();
        producer.registerMethodSendingCallback(tester);

        producer.start();

        final CommunicationEndpoint consumer = startConsumer(executor);

        Thread.sleep(1000);
        CompletableFuture<Map> response = producer.syncCallMethod(METHOD_1.withTimeout(100_000), singletonMap("aa", 11));

        assertEquals(1, response.get(3, TimeUnit.SECONDS).get("a"));

        tester.assertSentAndReceived();

        producer.stop();
        consumer.stop();
        executor.shutdown();
    }

    private CommunicationEndpoint startConsumer(ExecutorService executor) {
        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), MethodHandlingSuccessTest.targetConsumerIdentity, executor);

        consumer.registerMethodProcessor(SelfDescribingMethodProcessor.of(METHOD_1, (Map x) -> {
            LOGGER.info("Processing " + x);
            assertEquals(11, x.get("aa"));
            return singletonMap("a", 1);
        }));

        consumer.start();
        return consumer;
    }
}
