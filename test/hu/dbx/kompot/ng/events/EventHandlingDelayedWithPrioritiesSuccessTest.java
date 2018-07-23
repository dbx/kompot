package hu.dbx.kompot.ng.events;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.TestRedis;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.handler.SelfDescribingEventProcessor;
import hu.dbx.kompot.events.Priority;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.LoggerUtils;
import hu.dbx.kompot.producer.EventGroupProvider;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Elinditunk ket esemenyt. A szerver csak kesobb indul el es a magasabb prioritasuval kezdi a feldolgozast.
 */
@SuppressWarnings("unchecked")
public class EventHandlingDelayedWithPrioritiesSuccessTest {

    private static final EventDescriptor EVENT_1 = EventDescriptor.of("EVENT1", Map.class, Priority.LOW);
    private static final EventDescriptor EVENT_2 = EventDescriptor.of("EVENT2", Map.class, Priority.HIGH);

    private static final ConsumerIdentity consumerIdentity = groupGroup("EVENT1");
    private static final ConsumerIdentity producerIdentity = groupGroup("XXX");

    @Rule
    public TestRedis redis = TestRedis.build();

    @Test
    public void testSendManyEventsAndProcessInOrder() throws InterruptedException, SerializationException {
        final ExecutorService executor = Executors.newFixedThreadPool(4);

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.constantly("EVENT1"), producerIdentity, executor);
        producer.start();

        for (int i = 0; i < 10; i++) {
            producer.asyncSendEvent(EVENT_1, singletonMap("aa", 11));
            producer.asyncSendEvent(EVENT_2, singletonMap("bb", 11));
        }

        executor.awaitTermination(3, TimeUnit.SECONDS);

        final ConcurrentLinkedQueue<Integer> counter = new ConcurrentLinkedQueue();
        CommunicationEndpoint consumer = startConsumer(counter, executor);

        executor.awaitTermination(3, TimeUnit.SECONDS);

        assertEquals(20, counter.size());

        // jo, itt nem lehet elvarni, hogy pont sorban fognak megerkezni. de az elso-utolso stimmeljen!
        assertArrayEquals(new Object[]{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, counter.toArray());

        producer.stop();
        consumer.stop();
    }

    private CommunicationEndpoint startConsumer(ConcurrentLinkedQueue<Integer> counter, ExecutorService executor) {
        final EventGroupProvider egp = EventGroupProvider.constantly("X");
        final URI uri = redis.getConnectionURI();

        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(uri, egp, consumerIdentity, executor);

        consumer.registerEventHandler(SelfDescribingEventProcessor.of(EVENT_1, (data, meta) -> counter.add(1)));
        consumer.registerEventHandler(SelfDescribingEventProcessor.of(EVENT_2, (data, meta) -> counter.add(2)));

        consumer.start();
        return consumer;
    }
}
