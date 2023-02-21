package hu.dbx.kompot.ng.events;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.handler.SelfDescribingEventProcessor;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.LoggerUtils;
import hu.dbx.kompot.ng.AbstractRedisTest;
import hu.dbx.kompot.producer.EventGroupProvider;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static java.util.Collections.singletonMap;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Elinditunk egy esemenyt. A szerver csak kesobb indul el es csak kesobb kezdi el feldolgozni (sikeresen).
 */
@SuppressWarnings("unchecked")
public class EventHandlingDelayedSuccessTest extends AbstractRedisTest {
    private static final Logger LOGGER = LoggerUtils.getLogger();

    private static final EventDescriptor EVENT_1 = EventDescriptor.of("EVENT1", Map.class);
    private static final ConsumerIdentity consumerIdentity = groupGroup("EVENT1");
    private static final ConsumerIdentity producerIdentity = groupGroup("XXX");
    private static final int EVENT_COUNT = 100;

    @Test
    public void testEventsSentThenLaterReceived() throws InterruptedException, SerializationException {
        final ExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);
        producer.start();
        for (int i = 0; i < EVENT_COUNT; i++) {
            producer.asyncSendEvent(EVENT_1, singletonMap("aa", i));
        }
        producer.stop();

        final CopyOnWriteArrayList<Integer> counter = new CopyOnWriteArrayList();
        CommunicationEndpoint consumer = startConsumer(counter, executor);

        await().atMost(10, TimeUnit.SECONDS).until(() -> counter.size() == EVENT_COUNT);
        consumer.stop();
        executor.shutdown();
        await("Executor should be terminated").atMost(3, TimeUnit.SECONDS).until(executor::isTerminated);

        assertEquals(EVENT_COUNT, counter.size());

        final int middleEvent = counter.stream().skip(EVENT_COUNT / 2).findFirst().orElseThrow(() -> new RuntimeException("No value found in the middle"));
        final int lastEvent = counter.stream().skip(EVENT_COUNT - 1).findFirst().orElseThrow(() -> new RuntimeException("No event found at the end"));

        assertTrue("The last received events should have a higher order than the middle one.",
                lastEvent > middleEvent);
    }

    private CommunicationEndpoint startConsumer(List<Integer> counter, ExecutorService executor) {
        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), consumerIdentity, executor);
        consumer.registerEventHandler(SelfDescribingEventProcessor.of(EVENT_1, (data, meta, callback) -> {
            LOGGER.info("Processed");
            callback.success("OK");
            counter.add(((Map<String, Integer>) data).get("aa"));
        }));
        consumer.start();
        return consumer;
    }
}
