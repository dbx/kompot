package hu.dbx.kompot.ng.massive;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.handler.SelfDescribingEventProcessor;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.broadcast.handler.SelfDescribingBroadcastProcessor;
import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.consumer.sync.handler.SelfDescribingMethodProcessor;
import hu.dbx.kompot.ng.AbstractRedisTest;
import hu.dbx.kompot.producer.EventGroupProvider;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.awaitility.Awaitility.await;

@SuppressWarnings("java:S2925")
public class MassiveTest extends AbstractRedisTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MassiveTest.class);

    public static final int AGENT_COUNT = 10, EVENT_COUNT = 110, MESSAGE_COUNT = 120, BROADCAST_COUNT = 130;

    @Test
    public void test() throws InterruptedException {

        final CountDownLatch readyLatch = new CountDownLatch(AGENT_COUNT); // agents are ready to process messages
        final CountDownLatch startLatch = new CountDownLatch(1); // agents should start processing messages
        final CountDownLatch runningLatch = new CountDownLatch(AGENT_COUNT); // agents finished processing messages

        final ExecutorService service = Executors.newFixedThreadPool(AGENT_COUNT);

        final URI redisUri = redis.getConnectionURI();

        final AtomicInteger processedEvents = new AtomicInteger(0);
        final AtomicInteger processedMethods = new AtomicInteger(0);
        final AtomicInteger processedBroadcasts = new AtomicInteger(0);


        for (int i = 0; i < AGENT_COUNT; i++) {
            service.submit(new Agent(redisUri, readyLatch, startLatch, runningLatch, i, processedEvents, processedMethods, processedBroadcasts));
        }

        readyLatch.await();
        startLatch.countDown();

        LOGGER.debug("Waiting for agents to finish");
        service.shutdown();
        await("Executor should terminate").atMost(300, TimeUnit.SECONDS).until(service::isTerminated);

        LOGGER.debug("Agents finished");

        Assert.assertEquals("Unexpected processed event count", AGENT_COUNT * EVENT_COUNT, processedEvents.get());
        Assert.assertEquals("Unexpected processed method count", AGENT_COUNT * MESSAGE_COUNT, processedMethods.get());
        //every agent broadcasts to the other half of the agents: the two group sizes are (AGENT_COUNT / 2) and (AGENT_COUNT - AGENT_COUNT / 2) (odd agent count can cause the groups to be uneven)
        Assert.assertEquals("Unexpected processed broadcast count",
                2 * (AGENT_COUNT - AGENT_COUNT / 2) * (AGENT_COUNT / 2) * BROADCAST_COUNT, processedBroadcasts.get());

    }


    private static final class Agent implements Callable, ConsumerIdentity {

        Agent(final URI redisUri, final CountDownLatch readyLatch, CountDownLatch startLatch, CountDownLatch runningLatch, int nr, AtomicInteger processedEvents, final AtomicInteger processedMethods, final AtomicInteger processedBroadcasts) {
            this.redisUri = redisUri;
            this.readyLatch = readyLatch;
            this.startLatch = startLatch;
            this.runningLatch = runningLatch;
            this.nr = nr;
            this.processedEvents = processedEvents;
            this.processedMethods = processedMethods;
            this.processedBroadcasts = processedBroadcasts;
        }


        private final URI redisUri;
        private final CountDownLatch readyLatch;
        private final CountDownLatch startLatch;
        final CountDownLatch runningLatch;
        final int nr;
        final AtomicInteger processedEvents;
        private final AtomicInteger processedMethods;
        private final AtomicInteger processedBroadcasts;

        private final AtomicBoolean messageReceived = new AtomicBoolean(true); //if we receive a message, we set this flag true

        @Override
        public Object call() throws Exception {

            CommunicationEndpoint endpoint = CommunicationEndpoint.ofRedisConnectionUri(redisUri, PROVIDER, this);

            // feliratkozik egy random esemenyre
            endpoint.registerEventHandler(SelfDescribingEventProcessor.of(EVENT.get(nr % 2), (data) -> {
                try {
                    messageReceived.set(true);
                    Thread.sleep((long) (Math.random() * 100));

                    int eventNo = processedEvents.incrementAndGet();

                    LOGGER.debug("Executed event #{}", eventNo);

                    if (Math.random() < 0.1) throw new RuntimeException("Kacsa.");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));

            // feliratkozik egy random esemenyre
            endpoint.registerBroadcastProcessor(SelfDescribingBroadcastProcessor.of(BROADCAST.get(nr % 2), (data) -> {
                try {
                    messageReceived.set(true);
                    Thread.sleep((long) (Math.random() * 100));

                    int broadcastNo = processedBroadcasts.incrementAndGet();

                    LOGGER.debug("Executed broadcast #{}", broadcastNo);

                    if (Math.random() < 0.1) throw new RuntimeException("Kacsa.");

                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));

            endpoint.registerMethodProcessor(SelfDescribingMethodProcessor.of(METHOD.get(nr % 2), (data) -> {
                try {
                    messageReceived.set(true);
                    Thread.sleep((long) (Math.random() * 100));

                    int methodNo = processedMethods.incrementAndGet();

                    LOGGER.debug("Executed method #{}", methodNo);

                    if (Math.random() < 0.1) {
                        throw new RuntimeException("Kacsa.");
                    }

                    return emptyMap();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));

            endpoint.start();

            readyLatch.countDown();
            startLatch.await();


            for (int i = 0; i < EVENT_COUNT; i++) {
                endpoint.asyncSendEvent(EVENT.get((nr + 1) % 2), emptyMap());
            }
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                endpoint.syncCallMethod(METHOD.get((nr + 1) % 2), singletonMap("ali", "baba"));
            }
            for (int i = 0; i < BROADCAST_COUNT; i++) {
                endpoint.broadcast(BROADCAST.get((nr + 1) % 2), singletonMap("bc", 23));
            }

            //wait for messages to be all processed (no message received in 300ms)
            while (messageReceived.getAndSet(false)) {
                Thread.sleep(300);
            }

            endpoint.stop();
            runningLatch.countDown();

            System.out.println("Exited agent. still running: " + runningLatch.getCount());
            return null;
        }

        String identifier = UUID.randomUUID().toString();

        @Override
        public String getEventGroup() {
            return EVENT.get(nr % 2).getEventName();
        }

        @Override
        public String getIdentifier() {
            return identifier;
        }

        @Override
        public String getMessageGroup() {
            return METHOD.get(nr % 2).getMethodGroupName();
        }
    }

    public final static EventGroupProvider PROVIDER = marker -> singletonList(marker.getEventName());

    private final static List<BroadcastDescriptor<Map>> BROADCAST = asList(BroadcastDescriptor.of("BC1", Map.class), BroadcastDescriptor.of("BC2", Map.class));
    private final static List<MethodDescriptor<Map, Map>> METHOD = asList(MethodDescriptor.ofName("MG1", "METH1"), MethodDescriptor.ofName("MG2", "METH2"));
    private final static List<EventDescriptor<Map>> EVENT = asList(EventDescriptor.of("EVT", Map.class), EventDescriptor.of("EV2", Map.class));
}
