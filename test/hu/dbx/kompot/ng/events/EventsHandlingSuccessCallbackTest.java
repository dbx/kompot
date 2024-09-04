package hu.dbx.kompot.ng.events;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.EventFrame;
import hu.dbx.kompot.consumer.async.EventReceivingCallback;
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
import java.util.concurrent.atomic.AtomicInteger;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static java.util.Collections.singletonMap;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@SuppressWarnings("unchecked")
public class EventsHandlingSuccessCallbackTest extends AbstractRedisTest {
    private static final Logger LOGGER = LoggerUtils.getLogger();

    private static final EventDescriptor EVENT_1 = EventDescriptor.of("EVENT1X", Map.class);
    private static final ConsumerIdentity consumerIdentity = groupGroup("EVENT1X");
    private static final ConsumerIdentity producerIdentity = groupGroup("EVENTC");

    /**
     * Sikeresen elkuldunk es feldolgozunk 10 eventet.
     */
    @Test
    public void testSuccessfullyHandleEvent() throws InterruptedException, SerializationException {
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        final AtomicInteger counter = new AtomicInteger(0);

        final List<String> statuses = new CopyOnWriteArrayList<>();
        final CommunicationEndpoint consumer = startConsumer(statuses, counter, executor);

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);
        producer.start();
        for (int i = 0; i < 10; i++) {
            producer.asyncSendEvent(EVENT_1, singletonMap("aa", i));
        }

        await("10 events should be processed").atMost(10, TimeUnit.SECONDS).untilAtomic(counter, is(10));
        producer.stop();
        consumer.stop();
        executor.shutdown();
        await("Executor should be terminated").atMost(10, TimeUnit.SECONDS).until(executor::isTerminated);

        // should have received at least n times?
        assertEquals(10, statuses.stream().filter(x -> x.equals("RECEIVED")).count());
        assertEquals(10, statuses.stream().filter(x -> x.equals("SUCCESS")).count());
        assertEquals(0, statuses.stream().filter(x -> x.equals("FAILED")).count());
    }

    private CommunicationEndpoint startConsumer(List<String> status, AtomicInteger counter, ExecutorService executor) {
        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.empty(), consumerIdentity, executor);
        consumer.registerEventHandler(SelfDescribingEventProcessor.of(EVENT_1, (data, meta, callback) -> {
            counter.incrementAndGet();
            LOGGER.info("Test Callback Processed");
            callback.success("OK");
        }));
        consumer.registerEventReceivingCallback(new EventReceivingCallback() {
            @Override
            public void onEventReceived(EventFrame eventFrame) {
                assertNotNull(eventFrame);
                status.add("RECEIVED");
            }

            @Override
            public void onEventProcessedSuccessfully(EventFrame eventFrame, String message) {
                status.add("SUCCESS");
            }

            @Override
            public void onEventProcessingFailure(EventFrame eventFrame, Throwable t) {
                status.add("FAILED");
            }
        });
        consumer.start();
        return consumer;
    }
}
