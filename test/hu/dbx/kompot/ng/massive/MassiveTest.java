package hu.dbx.kompot.ng.massive;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.TestRedis;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.handler.SelfDescribingEventProcessor;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.broadcast.handler.SelfDescribingBroadcastProcessor;
import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.consumer.sync.handler.SelfDescribingMethodProcessor;
import hu.dbx.kompot.producer.EventGroupProvider;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static java.util.Collections.*;

public class MassiveTest {

    public static final int AGENT_COUNT = 10, EVENT_COUNT = 100, MESSAGE_COUNT = 100, BROADCAST_COUNT = 100;


    @Ignore
    @Test
    public void test() throws InterruptedException {

        final CountDownLatch agentsLatch = new CountDownLatch(AGENT_COUNT);

        final ExecutorService service = Executors.newFixedThreadPool(AGENT_COUNT);

        final AtomicInteger allEvents = new AtomicInteger(AGENT_COUNT * (EVENT_COUNT + MESSAGE_COUNT + BROADCAST_COUNT));

        for (int i = 0; i < AGENT_COUNT; i++) {
            service.submit(new Agent(agentsLatch, i, allEvents));
        }

        agentsLatch.await();
    }


    private final class Agent implements Callable, ConsumerIdentity {

        Agent(CountDownLatch latch, int nr, AtomicInteger allEvents) {
            this.agentsLatch = latch;
            this.nr = nr;
            this.allEvents = allEvents;
        }



        final CountDownLatch agentsLatch;
        final int nr;
        final AtomicInteger allEvents;

        @Override
        public Object call() throws Exception {

            TestRedis redis = new TestRedis();

            redis.before();

            CommunicationEndpoint endpoint = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), PROVIDER, this);

            // feliratkozik egy random esemenyre
            endpoint.registerEventHandler(SelfDescribingEventProcessor.of(EVENT.get(nr % 2), (data) -> {
                try {
                    Thread.sleep((long) (Math.random() * 100));

                    int remaining = allEvents.decrementAndGet();

                    System.out.println("Executed event!" + remaining);

                    if (Math.random() < 0.1) throw new RuntimeException("Kacsa.");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));

            // feliratkozik egy random esemenyre
            endpoint.registerBroadcastProcessor(SelfDescribingBroadcastProcessor.of(BROADCAST.get(nr % 2), (data) -> {
                try {
                    Thread.sleep((long) (Math.random() * 100));

                    int remaining = allEvents.decrementAndGet();

                    System.out.println("Executed broadcast!" + remaining);

                    if (Math.random() < 0.1) throw new RuntimeException("Kacsa.");

                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));

            endpoint.registerMethodProcessor(SelfDescribingMethodProcessor.of(METHOD.get(nr % 2), (data) -> {
                try {
                    Thread.sleep((long) (Math.random() * 100));

                    int remaining = allEvents.decrementAndGet();

                    System.out.println("Executed method!" + remaining);

                    if (Math.random() < 0.1) throw new RuntimeException("Kacsa.");

                    return emptyMap();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));

            endpoint.start();

            for (int i = 0; i < EVENT_COUNT; i++) {
                Thread.sleep(100);
                endpoint.asyncSendEvent(EVENT.get((nr + 1) % 2), emptyMap());
                endpoint.syncCallMethod(METHOD.get((nr + 1) % 2), singletonMap("ali", "baba"));
                endpoint.broadcast(BROADCAST.get((nr + 1) % 2), singletonMap("bc", 23));
            }

            while (allEvents.get() > nr * (EVENT_COUNT + MESSAGE_COUNT + BROADCAST_COUNT)) {
                Thread.sleep(100);
            }

            endpoint.stop();
            redis.after();

            agentsLatch.countDown();

            System.out.println("Exited agent. still running: " + agentsLatch.getCount());
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
    private final static List<EventDescriptor<Map>> EVENT = asList(EventDescriptor.of("EVT", Map.class),EventDescriptor.of("EV2", Map.class));
}
