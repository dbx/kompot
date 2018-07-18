package hu.dbx.kompot.ng.methods;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.TestRedis;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.consumer.sync.handler.SelfDescribingMethodProcessor;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.producer.EventGroupProvider;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.*;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@SuppressWarnings("unchecked")
public class MethodHandlingRemoteReturnsNullTest {

    private static final MethodDescriptor METHOD_1 = MethodDescriptor.ofName("GROUP1", "method1");
    private static final ConsumerIdentity consumerIdentity = groupGroup("GROUP1");
    private static final ConsumerIdentity producerIdentity = groupGroup("GROUP2");

    @Rule
    public TestRedis redis = TestRedis.build();

    /**
     * When response is a null value then we receive it.
     */
    @Test
    public void haNullValaszAkkorVisszakapom() throws InterruptedException, SerializationException, TimeoutException, ExecutionException {
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), consumerIdentity, executor);

        consumer.registerMethodProcessor(SelfDescribingMethodProcessor.of(METHOD_1, x -> null));
        consumer.start();

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);
        producer.start();

        CompletableFuture<Map> response = producer.syncCallMethod(METHOD_1.withTimeout(100_000), singletonMap("aa", 11));

        Map r = response.get(10, TimeUnit.SECONDS);

        assertNull(r);

        producer.stop();
        consumer.stop();
        executor.shutdown();
    }

    /**
     * But we must not send null as request data.
     */
    @Test
    public void failOnNulLRequestData() throws SerializationException {
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), consumerIdentity, executor);

        consumer.registerMethodProcessor(SelfDescribingMethodProcessor.of(METHOD_1, x -> {
            fail("must not happen!");
            return 1;
        }));
        consumer.start();

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);
        producer.start();

        try {
            producer.syncCallMethod(METHOD_1.withTimeout(100_000), null);
            fail("Should have thrown!");
        } catch (IllegalArgumentException __) {
            // elvart mukodes!
        }

        producer.stop();
        consumer.stop();
        executor.shutdown();
    }
}
