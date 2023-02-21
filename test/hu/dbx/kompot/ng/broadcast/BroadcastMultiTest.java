package hu.dbx.kompot.ng.broadcast;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.broadcast.handler.SelfDescribingBroadcastProcessor;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.ng.AbstractRedisTest;
import hu.dbx.kompot.producer.EventGroupProvider;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static java.util.stream.Collectors.toList;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;

/**
 * Kikuldunk egy broadcast esemenyt es ellenorizzuk, hogy mind a negy feliratkozo megkapta.
 */
public class BroadcastMultiTest extends AbstractRedisTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(BroadcastMultiTest.class);

    private static final BroadcastDescriptor<String> BROADCAST1 = BroadcastDescriptor.of("BC1", String.class);

    private static final ConsumerIdentity serverIdentity = groupGroup("Consumer");

    private static final int CONSUMER_COUNT = 30;

    @Test
    public void testSuccessfullyHandleEvent() throws SerializationException, InterruptedException {
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        final AtomicInteger counter = new AtomicInteger(0);

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), serverIdentity, executor);

        producer.start();

        List<CommunicationEndpoint> consumers = IntStream.rangeClosed(1, CONSUMER_COUNT).mapToObj(__ -> startConsumer(executor, counter)).collect(toList());

        // kikuldunk egy broadcast esemenyt
        producer.broadcast(BROADCAST1, "msg1");

        producer.stop();

        await("Counter should reach consumer count").atMost(5, TimeUnit.SECONDS).untilAtomic(counter, is(CONSUMER_COUNT));

        consumers.forEach(CommunicationEndpoint::stop);

        executor.shutdown();
        await("Executor should terminate").atMost(5, TimeUnit.SECONDS).until(executor::isTerminated);
    }

    private CommunicationEndpoint startConsumer(ExecutorService executor, AtomicInteger counter) {
        final CommunicationEndpoint server = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), serverIdentity, executor);

        server.registerBroadcastProcessor(SelfDescribingBroadcastProcessor.of(BROADCAST1, x -> {
            LOGGER.debug("Received broadcast: {}", x);
            counter.incrementAndGet();
        }));
        server.start();

        return server;
    }
}
