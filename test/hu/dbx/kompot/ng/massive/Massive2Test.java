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
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.producer.EventGroupProvider;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

@Ignore
public class Massive2Test {

    @Test
    public void test() throws InterruptedException {
        new Massive2Test().runTest();
    }

    public static final int AGENT_COUNT = 10, EVENT_COUNT = 500, MESSAGE_COUNT = 0, BROADCAST_COUNT = 0;
    Set<Long> methods = new HashSet<>(); // a szal azonositok amiken

    // to process.
    final Set<UUID> unprocessedEvents = new ConcurrentSkipListSet<>();
    final Set<UUID> unprocessedMethods = new ConcurrentSkipListSet<>();

    // for stopping agents.
    final CountDownLatch agentsLatch = new CountDownLatch(AGENT_COUNT);
    final CountDownLatch agentsMaybeFinished = new CountDownLatch(AGENT_COUNT);

    final AtomicInteger successfullyStarted = new AtomicInteger(0);

    public void runTest() throws InterruptedException {

        final ExecutorService service = Executors.newFixedThreadPool(AGENT_COUNT + 2);

        final AtomicInteger allEvents = new AtomicInteger(AGENT_COUNT * (EVENT_COUNT + MESSAGE_COUNT + BROADCAST_COUNT));


        for (int i = 0; i < AGENT_COUNT; i++) {
            service.submit(new Agent(agentsLatch, i));
        }

        agentsLatch.await();

    }

    private void processEvent(UUID evt) {
        if (!unprocessedEvents.remove(evt))
            throw new RuntimeException("Could not remove!");
    }

    private void processMethod(UUID mth) {
        unprocessedMethods.remove(mth);
    }

    private final class Agent implements Callable, ConsumerIdentity {

        Agent(CountDownLatch latch, int nr) {
            this.agentsLatch = latch;
            this.sending = nr % 2;
            this.receiving = (nr + 1) % 2;
            endpoint = CommunicationEndpoint.ofRedisConnectionUri(TestRedis.getDefaultUri(), PROVIDER, this);
        }

        final CountDownLatch agentsLatch;
        final int sending, receiving;
        final CommunicationEndpoint endpoint;

        @Override
        public Object call() throws Exception {
            try {

                endpoint.registerEventHandler(SelfDescribingEventProcessor.of(EVENT.get(receiving), (data) -> {
                    try {
                        Thread.sleep((long) (Math.random() * 100));

                        processEvent(UUID.fromString(data.get("request").toString()));

                        if (Math.random() < 0.5) throw new RuntimeException("Kacsa.");
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }));

                endpoint.registerBroadcastProcessor(SelfDescribingBroadcastProcessor.of(BROADCAST.get(receiving), (data) -> {
                    try {
                        Thread.sleep((long) (Math.random() * 100));
                        System.out.println("Executed broadcast!");
                        if (Math.random() < 0.5)
                            throw new RuntimeException("Broadcast Kacsa.");


                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }));

                endpoint.registerMethodProcessor(SelfDescribingMethodProcessor.of(METHOD.get(receiving), (data) -> {
                    try {
                        Thread.sleep((long) (Math.random() * 20));
                        if (Math.random() < 0.5) throw new RuntimeException("Kacsa.");
                        return singletonMap("response", data.get("request"));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }));

                endpoint.start();
                successfullyStarted.incrementAndGet();

                System.out.println("Started! " + Thread.currentThread().getId());

                for (int i = 0; i < EVENT_COUNT; i++) {
                    //Thread.sleep(100)
                    System.out.println(Thread.currentThread().getId() + " Step " + i);

                    if (Math.random() < 0.3333) {
                        sendMethodCall();
                    } else if (Math.random() < 0.5) {
                        sendEventCall();
                    } else {
                        sendBroadcast();
                    }
                }
                System.out.println("After loop!" + Thread.currentThread().getId());

            } catch (Throwable t) {
                System.out.println("Hibaval kileptunk!");
                t.printStackTrace();
                System.exit(34);
            }


            System.out.println("Counting down. Already started: " + successfullyStarted.get()); // 10


            agentsMaybeFinished.countDown();
            agentsMaybeFinished.await();
            System.out.println("Counted down.");

            while (!unprocessedMethods.isEmpty() || !unprocessedMethods.isEmpty()) {
                System.out.println("Ok: " + unprocessedEvents.size() + " " + unprocessedMethods.size());
                Thread.sleep(100);
            }

            endpoint.stop();
            agentsLatch.countDown();

            return null;
        }


        private void sendMethodCall() {
            UUID mth = UUID.randomUUID();
            long before = System.currentTimeMillis();
            try {
                methods.add(Thread.currentThread().getId());
                System.out.println("Sending method on " + Thread.currentThread().getId());
                unprocessedMethods.add(mth);
                endpoint.syncCallMethod(METHOD.get(sending), singletonMap("ali", "baba")).get(10, TimeUnit.SECONDS);
                processMethod(mth);
            } catch (TimeoutException to) {
                System.err.println("hahaha");
                System.exit(-1);
            } catch (CancellationException ce) {
                System.err.println("Cancelled!" + ce.getCause());
                processMethod(mth);
            } catch (ExecutionException ce) {
                System.err.println("Exec err: " + ce.getCause());
                processMethod(mth);
            } catch (Throwable t) {
                System.err.println("Varatlan hiba!");
                System.exit(-1);
            } finally {
                System.out.println("End of method on " + Thread.currentThread().getId());
                methods.remove(Thread.currentThread().getId());
            }
        }

        private void sendEventCall() throws InterruptedException, ExecutionException, hu.dbx.kompot.exceptions.SerializationException {
            final UUID evt = UUID.randomUUID();
            unprocessedMethods.add(evt);
            endpoint.asyncSendEvent(EVENT.get(sending), singletonMap("request", evt));
        }

        private void sendBroadcast() throws SerializationException {
            endpoint.broadcast(BROADCAST.get(sending), singletonMap("request", "asd"));

        }


        String identifier = UUID.randomUUID().toString();

        @Override
        public String getEventGroup() {
            return EVENT.get(receiving).getEventName();
        }

        @Override
        public String getIdentifier() {
            return identifier;
        }

        @Override
        public String getMessageGroup() {
            return METHOD.get(receiving).getMethodGroupName();
        }
    }

    public final static EventGroupProvider PROVIDER = marker -> singletonList(marker.getEventName());

    private final static List<BroadcastDescriptor<Map>> BROADCAST = asList(BroadcastDescriptor.of("BC1", Map.class), BroadcastDescriptor.of("BC2", Map.class));
    private final static List<MethodDescriptor<Map, Map>> METHOD = asList(MethodDescriptor.ofName("MG1", "METH1").withTimeout(50), MethodDescriptor.ofName("MG2", "METH2").withTimeout(50));
    private final static List<EventDescriptor<Map>> EVENT = asList(EventDescriptor.of("EV1", Map.class), EventDescriptor.of("EV2", Map.class));
}
