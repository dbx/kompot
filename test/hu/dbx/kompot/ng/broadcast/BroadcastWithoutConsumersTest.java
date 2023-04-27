package hu.dbx.kompot.ng.broadcast;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.ng.AbstractRedisTest;
import hu.dbx.kompot.producer.EventGroupProvider;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static org.junit.Assert.assertTrue;

/**
 * El tudunk kuldeni sok broadcast esemenyt ugy igy, hogy nincsen consumer, aki figyelne ra.
 */
public class BroadcastWithoutConsumersTest extends AbstractRedisTest {


    private static final BroadcastDescriptor<String> BROADCAST1 = BroadcastDescriptor.of("BC1", String.class);

    private static final ConsumerIdentity serverIdentity = groupGroup("Consumer");

    @Test
    public void testEventsAreUnhandled() {
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), serverIdentity, executor);
        producer.start();

        Stream.of(1, 2, 3, 4, 5, 6).forEach(__ -> {
            try {
                producer.broadcast(BROADCAST1, "msg1");
            } catch (SerializationException e) {
                throw new RuntimeException(e);
            }
        });

        producer.stop();
        executor.shutdown();

        assertTrue(true);
    }
}
