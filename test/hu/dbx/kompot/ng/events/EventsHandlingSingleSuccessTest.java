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

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static java.util.Collections.singletonMap;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;

/**
 * Sikeresen feldobunk egy feldolgozunk EGY darab esemenyt.
 */
@SuppressWarnings("unchecked")
public class EventsHandlingSingleSuccessTest extends AbstractRedisTest {
    private static final Logger LOGGER = LoggerUtils.getLogger();

    private static final String EVENT_CODE = "EVENT1" + EventsHandlingSingleSuccessTest.class.getName();

    private static final EventDescriptor EVENT_1 = EventDescriptor.of(EVENT_CODE, Map.class);
    private static final ConsumerIdentity consumerIdentity = groupGroup(
            "EVENT1" + EventsHandlingSingleSuccessTest.class.getName());
    private static final ConsumerIdentity producerIdentity = groupGroup(
            "UNUSED" + EventsHandlingSingleSuccessTest.class.getName());

    @Test
    public void test() throws InterruptedException, SerializationException {
        final ExecutorService executor = Executors.newFixedThreadPool(4);

        final AtomicInteger counter = new AtomicInteger(0);
        final CommunicationEndpoint consumer = startConsumer(counter, executor);
        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);

        producer.start();
        producer.asyncSendEvent(EVENT_1, singletonMap("aa", 12));

        await("Consumer should process the event").atMost(5, TimeUnit.SECONDS).untilAtomic(counter, is(1));
        producer.stop();
        consumer.stop();
        executor.shutdown();
        await("Executor should terminate").atMost(5, TimeUnit.SECONDS).until(executor::isTerminated);
    }

    private CommunicationEndpoint startConsumer(AtomicInteger counter, ExecutorService executor) {
        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.empty(), consumerIdentity, executor);
        consumer.registerEventHandler(SelfDescribingEventProcessor.of(EVENT_1, (data, meta, callback) -> {
            counter.incrementAndGet();
            LOGGER.info("Test Callback Processed");
            callback.success("OK");
        }));
        consumer.start();
        return consumer;
    }
}
