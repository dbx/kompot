package hu.dbx.kompot.ng.events;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.TestRedis;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.handler.SelfDescribingEventProcessor;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.LoggerUtils;
import hu.dbx.kompot.producer.EventGroupProvider;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

/**
 * Elinditunk egy esemenyt. A szerver csak kesobb indul el es csak kesobb kezdi el feldolgozni (sikeresen).
 */
@SuppressWarnings("unchecked")
public class EventHandlingDelayedSuccessTest {
    private static final Logger LOGGER = LoggerUtils.getLogger();

    private static final EventDescriptor EVENT_1 = EventDescriptor.of("EVENT1", Map.class);
    private static final ConsumerIdentity consumerIdentity = groupGroup("EVENT1");
    private static final ConsumerIdentity producerIdentity = groupGroup("XXX");

    @Rule
    public TestRedis redis = TestRedis.build();

    @Test
    public void testEventsSentThenLaterReceived() throws InterruptedException, SerializationException {
        final ExecutorService executor = Executors.newFixedThreadPool(4);

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);
        producer.start();
        for (int i = 0; i < 10; i++) {
            producer.asyncSendEvent(EVENT_1, singletonMap("aa", 11));
        }

        executor.awaitTermination(3, TimeUnit.SECONDS);

        final AtomicInteger counter = new AtomicInteger(0);
        CommunicationEndpoint consumer = startConsumer(counter, executor);


        assertEquals(counter.get(), 0);

        executor.awaitTermination(3, TimeUnit.SECONDS);

        assertEquals(10, counter.get());

        producer.stop();
        consumer.stop();
    }

    private CommunicationEndpoint startConsumer(AtomicInteger counter, ExecutorService executor) {
        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), consumerIdentity, executor);
        consumer.registerEventHandler(SelfDescribingEventProcessor.of(EVENT_1, (data, callback) -> {
            LOGGER.info("Processed");
            callback.success("OK");
            counter.incrementAndGet();
        }));
        consumer.start();
        return consumer;
    }
}
