package hu.dbx.kompot.ng.methods;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.consumer.sync.handler.SelfDescribingMethodProcessor;
import hu.dbx.kompot.exceptions.MessageErrorResultException;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.ng.AbstractRedisTest;
import hu.dbx.kompot.producer.EventGroupProvider;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.*;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * A tuloldalon kivetel kepzodott.
 */
@SuppressWarnings("unchecked")
public class MethodHandlingRemoteExceptionTest extends AbstractRedisTest {

    private static final MethodDescriptor METHOD_1 = MethodDescriptor.ofName("GROUP1", "method1");
    private static final ConsumerIdentity consumerIdentity = groupGroup("GROUP1");
    private static final ConsumerIdentity producerIdentity = groupGroup("GROUP2");

    private static String EXCEPTION_MSG = "Now, I am become Death, the destroyer of worlds";

    @Test
    public void testHaHibaTortenikAkkorMegkapom() throws InterruptedException, SerializationException {
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), consumerIdentity, executor);

        consumer.registerMethodProcessor(SelfDescribingMethodProcessor.of(METHOD_1, x -> {
            throw new RuntimeException(EXCEPTION_MSG);
        }));
        consumer.start();

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);
        producer.start();

        CompletableFuture<Map> response = producer.syncCallMethod(METHOD_1.withTimeout(100_000), singletonMap("aa", 11));

        try {
            response.get(10, TimeUnit.SECONDS);
            fail();
        } catch (TimeoutException e) {
            fail();
        } catch (ExecutionException ignore) {
            ignore.printStackTrace();
            final MessageErrorResultException cause = (MessageErrorResultException) ignore.getCause();
            assertEquals(EXCEPTION_MSG, cause.getMessage());
            assertEquals("java.lang.RuntimeException", cause.getExceptionClass());
        }

        producer.stop();
        consumer.stop();
        executor.shutdown();
    }
}
