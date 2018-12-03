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
 * Send a lot of events, process them then send an other load of events.
 * <p>
 * All events should get processed.
 */
public class MassiveEventsTest {


    private static final int EVENT_COUNT = 1000;
    private static final String EVENT_NAME = UUID.randomUUID().toString();
    private static final EventDescriptor<Map> EVENT1 = EventDescriptor.of(EVENT_NAME, Map.class);
    private static final ConsumerIdentity serverIdentity = groupGroup(EVENT_NAME);
    private static final EventGroupProvider PROVIDER = EventGroupProvider.identity();

    private final ExecutorService executor = Executors.newFixedThreadPool(4);


    @Before
    public void before() throws IOException, URISyntaxException {
        new TestRedis().before();
    }

    @Test
    public void testcd() throws Exception {
        final CountDownLatch remainingEvents = new CountDownLatch(EVENT_COUNT);

        sendEvents(executor);

        // state 2 - processing all events from multiple agents
        startConsumer(remainingEvents);

        assertEquals(0L, remainingEvents.getCount());
    }

    private static void startConsumer(CountDownLatch remainingEvents) throws IOException, URISyntaxException, InterruptedException, SerializationException {
        final CommunicationEndpoint receiver = CommunicationEndpoint.ofRedisConnectionUri(new TestRedis().getConnectionURI(), PROVIDER, serverIdentity);
        receiver.registerEventHandler(SelfDescribingEventProcessor.of(EVENT1,
                (x) -> {
                    remainingEvents.countDown();
                    if (Math.random() > 0.6) {
                        throw new Error("asd");
                    }
                }
        ));

        System.out.println("Has this many events: " + remainingEvents.getCount());
        receiver.start();
        System.out.println("Started receiver...");

        remainingEvents.await();

        Thread.sleep(500L);

        final CountDownLatch phase2Events = new CountDownLatch(EVENT_COUNT);

        receiver.registerEventHandler(SelfDescribingEventProcessor.of(EVENT1,
                (x) -> {
                    phase2Events.countDown();
                    if (Math.random() > 0.6) {
                        throw new RuntimeException("asd");
                    }
                }
        ));

        sendPhase2Events(phase2Events);
        phase2Events.await();

        receiver.stop();
    }

    private static void sendPhase2Events(CountDownLatch phase2) throws IOException, URISyntaxException, hu.dbx.kompot.exceptions.SerializationException {
        final TestRedis redis = new TestRedis();

        // stage 1 - sending all events

        long total = phase2.getCount();

        final ConsumerIdentity senderIdentity = groupGroup("Sender");
        final CommunicationEndpoint sender = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), PROVIDER, senderIdentity);
        sender.start();
        for (int i = 0; i < total; i++) {
            sender.asyncSendEvent(EVENT1, singletonMap("a", 2));
        }
        sender.stop();
    }

    private static void sendEvents(ExecutorService executor) throws IOException, URISyntaxException, hu.dbx.kompot.exceptions.SerializationException, InterruptedException {
        final TestRedis redis = new TestRedis();

        // stage 1 - sending all events

        final ConsumerIdentity senderIdentity = groupGroup("Sender");
        final CommunicationEndpoint sender = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), PROVIDER, senderIdentity, executor);
        sender.start();
        for (int i = 0; i < EVENT_COUNT; i++) {
            sender.asyncSendEvent(EVENT1, singletonMap("a", 2));
        }
        sender.stop();
        executor.awaitTermination(3, TimeUnit.SECONDS);
    }
}
