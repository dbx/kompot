package hu.dbx.kompot.ng.broadcast;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.TestRedis;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.broadcast.handler.SelfDescribingBroadcastProcessor;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.producer.EventGroupProvider;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Kikuldunk egy broadcast esemenyt es ellenorizzuk, hogy mind a negy feliratkozo megkapta.
 */
public class BroadcastMultiTest {

    private static final BroadcastDescriptor<String> BROADCAST1 = BroadcastDescriptor.of("BC1", String.class);

    private static final ConsumerIdentity serverIdentity = groupGroup("Consumer");

    @Rule
    public TestRedis redis = TestRedis.build();

    @Test
    public void testSuccessfullyHandleEvent() throws SerializationException, InterruptedException {
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        final AtomicLong counter = new AtomicLong(0);

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), serverIdentity, executor);

        producer.start();

        List<CommunicationEndpoint> consumers = Stream.of(1, 2, 3, 4).map(__ -> startConsumer(executor, counter)).collect(toList());

        // kikuldunk egy broadcast esemenyt
        producer.broadcast(BROADCAST1, "msg1");

        Thread.sleep(1000);
        producer.stop();

        consumers.forEach(CommunicationEndpoint::stop);

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        assertEquals(4L, counter.get());
    }

    private CommunicationEndpoint startConsumer(ExecutorService executor, AtomicLong counter) {
        final CommunicationEndpoint server = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), serverIdentity, executor);

        server.registerBroadcastProcessor(SelfDescribingBroadcastProcessor.of(BROADCAST1, x -> counter.incrementAndGet()));
        server.start();

        return server;
    }
}
