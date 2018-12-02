package hu.dbx.kompot.ng.massive;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.TestRedis;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.handler.SelfDescribingEventProcessor;
import hu.dbx.kompot.impl.DefaultConsumerIdentity;
import hu.dbx.kompot.producer.EventGroupProvider;
import org.junit.Test;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;

/**
 * Tests massive event sending.
 */
public class MassiveEventsTest {

    private static final int EVENT_COUNT = 1;
    private static final String EVENT_NAME = UUID.randomUUID().toString();
    private static final EventDescriptor<Map> EVENT1 = EventDescriptor.of(EVENT_NAME, Map.class);
    private static final ConsumerIdentity serverIdentity = DefaultConsumerIdentity.groupGroup(EVENT_NAME);
    private static final EventGroupProvider PROVIDER = EventGroupProvider.identity();

    @Test
    public void test1() throws Exception {
        final CountDownLatch remainingEvents = new CountDownLatch(EVENT_COUNT);

        final TestRedis redis = new TestRedis();

        // stage 1 - sending all events

        final CommunicationEndpoint sender = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), PROVIDER, serverIdentity);
        sender.start();
        for (int i = 0; i < EVENT_COUNT; i++) {
            sender.asyncSendEvent(EVENT1, singletonMap("a", 2));
        }
        sender.stop();

        // state 2 - processing all events from multiple agents

        final CommunicationEndpoint receiver = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), PROVIDER, serverIdentity);
        receiver.registerEventHandler(SelfDescribingEventProcessor.of(EVENT1,
                (x) -> {
                    System.out.println("Counting");
                    remainingEvents.countDown();
                    System.out.println("Still has this many events: " + remainingEvents.getCount());
                }
        ));

        System.out.println("Has this many events: " + remainingEvents.getCount());
        receiver.start();
        System.out.println("Started receiver...");

        remainingEvents.await(5, TimeUnit.SECONDS);
        receiver.stop();
    }
}
