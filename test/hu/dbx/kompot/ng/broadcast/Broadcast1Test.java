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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static org.junit.Assert.assertEquals;

/**
 * Elkuldtunk egy broadcast uzenetet, valaki figyelt is ra.
 */
public class Broadcast1Test {


    private static final BroadcastDescriptor<String> BROADCAST1 = BroadcastDescriptor.of("BC1", String.class);

    private static final ConsumerIdentity serverIdentity = groupGroup("Consumer");

    @Rule
    public TestRedis redis = TestRedis.build();

    @Test
    public void testSuccessfullyHandleEvent() throws SerializationException, InterruptedException {
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), serverIdentity, executor);

        final AtomicLong counter = new AtomicLong(0);

        consumer.registerBroadcastProcessor(SelfDescribingBroadcastProcessor.of(BROADCAST1, x -> counter.incrementAndGet()));
        consumer.start();


        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), serverIdentity, executor);
        producer.start();

        // kikuldunk egy broadcast esemenyt
        producer.broadcast(BROADCAST1, "msg1");

        Thread.sleep(1000);
        producer.stop();
        consumer.stop();
        executor.shutdown();

        assertEquals(1L, counter.get());
    }
}
