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


    private static final int EVENT_COUNT = 400;
    private static final String EVENT_NAME = UUID.randomUUID().toString();
    private static final EventDescriptor<Map> EVENT1 = EventDescriptor.of(EVENT_NAME, Map.class);
    private static final ConsumerIdentity serverIdentity = groupGroup(EVENT_NAME);


    private static final String RECEIVER_GROUP = "RECEIVER_GROUP";
    private static final EventGroupProvider PROVIDER = EventGroupProvider.constantly(RECEIVER_GROUP);

    @Before
    public void before() throws IOException, URISyntaxException {
        new TestRedis().before();
    }

    @Test
    public void testcd() throws Exception {
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
                    try {
                        /// XXX: BUG: 
                        // ha felveszem a timeout-ot 10ms-re akkor jo lesz
                        // egyebkent egy ido utan nem dolgozza fel az esemenyeket.
                        Thread.sleep(1L)
                        ;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    remainingEvents.countDown();
                    System.out.println("Has this many left: " + remainingEvents.getCount());
                    if (Math.random() > 0.6) {
                        throw new Error("asd");
                    }
                }
        ));

        System.out.println("Has this many events: " + remainingEvents.getCount());
        receiver.start();
        System.out.println("Started receiver...");

        remainingEvents.await();
        System.out.println("Ended...");

        /*
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
*/
        receiver.stop();
    }

    /*
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
*/


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
