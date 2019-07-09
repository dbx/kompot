package hu.dbx.kompot.ng.events;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.TestRedis;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.handler.SelfDescribingEventProcessor;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.LoggerUtils;
import hu.dbx.kompot.moby.MetaDataHolder;
import hu.dbx.kompot.producer.EventGroupProvider;
import hu.dbx.kompot.producer.ProducerIdentity;
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
import static org.junit.Assert.assertNotEquals;

@SuppressWarnings("unchecked")
public class EventsHandlingSuccessTest {
    private static final Logger LOGGER = LoggerUtils.getLogger();

    private static final EventDescriptor EVENT_1 = EventDescriptor.of("EVENT123", Map.class);
    private static final ConsumerIdentity sourceConsumerIdentity = groupGroup("EVENTCProducer");
    private static final ProducerIdentity sourceProducerIdentity = new ProducerIdentity.DetailedIdentity("PROD_MOD", "v1");
    private static final ConsumerIdentity targetConsumerIdentity = groupGroup("EVENT123");

    @Rule
    public TestRedis redis = TestRedis.build();

    /**
     * Sikeresen elkuldunk es feldolgozunk 10 eventet.
     */
    @Test
    public void testSuccessfullyHandleEvent() throws InterruptedException, SerializationException {
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        final AtomicInteger counter = new AtomicInteger(0);
        final CommunicationEndpoint targetModule = startTarget(counter, executor);

        final CommunicationEndpoint sourceModule = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(),
                EventGroupProvider.identity(),
                sourceConsumerIdentity,
                sourceProducerIdentity,
                executor);
        sourceModule.start();
        for (int i = 0; i < 10; i++) {
            sourceModule.asyncSendEvent(EVENT_1, singletonMap("aa", i), MetaDataHolder.build("cid", "uref", "sourceName", 1234L));
        }

        Thread.sleep(1000);
        assertNotEquals(0, counter.get());

        sourceModule.stop();
        targetModule.stop();
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        assertEquals(10, counter.get());
    }

    private CommunicationEndpoint startTarget(AtomicInteger counter, ExecutorService executor) {
        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.empty(), targetConsumerIdentity, executor);
        consumer.registerEventHandler(SelfDescribingEventProcessor.of(EVENT_1, (data, meta, callback) -> {
            counter.incrementAndGet();
            LOGGER.info("Test Callback Processed");
            callback.success("OK");
        }));
        consumer.start();
        return consumer;
    }
}
