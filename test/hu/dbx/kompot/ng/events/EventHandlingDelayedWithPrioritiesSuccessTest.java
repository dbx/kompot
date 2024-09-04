package hu.dbx.kompot.ng.events;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.handler.SelfDescribingEventProcessor;
import hu.dbx.kompot.events.Priority;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.ng.AbstractRedisTest;
import hu.dbx.kompot.producer.EventGroupProvider;
import org.junit.Test;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static java.util.Collections.singletonMap;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

/**
 * Elinditunk ket esemenyt. A szerver csak kesobb indul el es a magasabb prioritasuval kezdi a feldolgozast.
 */
@SuppressWarnings("unchecked")
public class EventHandlingDelayedWithPrioritiesSuccessTest extends AbstractRedisTest {

    private static final EventDescriptor EVENT_1 = EventDescriptor.of("EVENT1D", Map.class, Priority.LOW);
    private static final EventDescriptor EVENT_2 = EventDescriptor.of("EVENT2", Map.class, Priority.HIGH);

    private static final ConsumerIdentity consumerIdentity = groupGroup("EVENT1D");
    private static final ConsumerIdentity producerIdentity = groupGroup("XXX");

    @Test
    public void testSendManyEventsAndProcessInOrder() throws InterruptedException, SerializationException {
        final ExecutorService executor = Executors.newFixedThreadPool(4);

        final EventGroupProvider provider = EventGroupProvider.constantly(EVENT_1.getEventName());
        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), provider, producerIdentity, executor);
        producer.start();

        for (int i = 0; i < 10; i++) {
            producer.asyncSendEvent(EVENT_1, singletonMap("aa", 11));
            producer.asyncSendEvent(EVENT_2, singletonMap("bb", 11));
        }

        final ConcurrentLinkedQueue<Integer> counter = new ConcurrentLinkedQueue();
        CommunicationEndpoint consumer = startConsumer(counter, executor);

        await("Consumers should process all events").atMost(5, TimeUnit.SECONDS).until(() -> counter.size() == 20);

        // itt nem lehet elvarni, hogy pont sorban fognak megerkezni. de az elso-utolso stimmeljen!
        assertEquals(2, counter.toArray()[0]);
        assertEquals(1, counter.toArray()[19]);

        producer.stop();
        consumer.stop();
        executor.shutdown();
        await("Executor should terminate").atMost(5, TimeUnit.SECONDS).until(executor::isTerminated);
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
