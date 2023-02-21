package hu.dbx.kompot.ng.broadcast;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.broadcast.handler.SelfDescribingBroadcastProcessor;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.ng.AbstractRedisTest;
import hu.dbx.kompot.producer.EventGroupProvider;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static org.awaitility.Awaitility.await;

/**
 * Elkuldtunk egy broadcast uzenetet, valaki figyelt is ra.
 */
public class Broadcast1Test extends AbstractRedisTest {


    private static final BroadcastDescriptor<String> BROADCAST1 = BroadcastDescriptor.of("BC1", String.class);

    private static final ConsumerIdentity serverIdentity = groupGroup("Consumer");

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

        await("Consumer should process the event").atMost(1, TimeUnit.SECONDS).untilAtomic(counter, Matchers.is(1L));
        producer.stop();
        consumer.stop();
        executor.shutdown();
        await("Executor should terminate").atMost(10, TimeUnit.SECONDS).until(executor::isTerminated);
    }
}
