package hu.dbx.kompot.ng.massive;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.TestRedis;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.handler.SelfDescribingEventProcessor;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.producer.EventGroupProvider;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static java.util.Collections.singletonMap;
import static junit.framework.TestCase.assertEquals;

/**
 * Tests the processing of lots of quick events.
 * <p>
 * 1. Send out a lot of small events then stop producer.
 * 2. Start consumer then process all the small events.
 * 3. Each event processing step is extremely quick (<1ms)
 * 4. All events should get processed eventually.
 * <p>
 * We check here for race conditions. On some occasions events used to get stuck when event processing was too quick.
 */
public class LotsOfQuickEventHandlingTest {


    private static final int EVENT_COUNT = 1000;
    private static final String EVENT_NAME = UUID.randomUUID().toString();
    private static final EventDescriptor<Map> EVENT1 = EventDescriptor.of(EVENT_NAME, Map.class);

    private static final String RECEIVER_GROUP = "RECEIVER_GROUP";
    private static final EventGroupProvider PROVIDER = EventGroupProvider.constantly(RECEIVER_GROUP);

    @Before
    public void before() throws IOException, URISyntaxException {
        new TestRedis().before();
    }

    @Test
    public void testMassiveEventsQuickPostprocess() throws Exception {
        final CountDownLatch remainingEvents = new CountDownLatch(EVENT_COUNT);

        sendInitialEvents();

        // state 2 - processing all events from multiple agents
        consumeInitialEvents(remainingEvents);

        assertEquals(0L, remainingEvents.getCount());
    }

    private static void consumeInitialEvents(CountDownLatch remainingEvents) throws IOException, URISyntaxException, InterruptedException, SerializationException {
        final CommunicationEndpoint receiver = CommunicationEndpoint.ofRedisConnectionUri(new TestRedis().getConnectionURI(), PROVIDER, groupGroup(RECEIVER_GROUP));
        receiver.registerEventHandler(SelfDescribingEventProcessor.of(EVENT1,
                (x) -> {
                    // this handler is extremely quick

                    remainingEvents.countDown();
                    if (Math.random() > 0.6) {
                        throw new Error("asd");
                    }
                }
        ));

        receiver.start();
        remainingEvents.await();

        receiver.stop();
    }

    /**
     * Send an initial set of events.
     */
    private static void sendInitialEvents() throws IOException, URISyntaxException, hu.dbx.kompot.exceptions.SerializationException, InterruptedException {
        final TestRedis redis = new TestRedis();

        final ExecutorService executor = Executors.newFixedThreadPool(4);

        // stage 1 - sending all events

        final ConsumerIdentity senderIdentity = groupGroup("Sender");
        final CommunicationEndpoint sender = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), PROVIDER, senderIdentity, executor);
        sender.start();
        for (int i = 0; i < EVENT_COUNT; i++) {
            sender.asyncSendEvent(EVENT1, singletonMap("a", 2));
        }
        executor.awaitTermination(6, TimeUnit.SECONDS);
        sender.stop();
    }
}
