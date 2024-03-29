package hu.dbx.kompot.ng.methods;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.consumer.sync.handler.SelfDescribingMethodProcessor;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.ng.AbstractRedisTest;
import hu.dbx.kompot.producer.EventGroupProvider;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static java.util.Collections.singletonMap;
import static junit.framework.TestCase.assertTrue;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Nem fut a szerver, csak a kliens. Timeout Exception-nal meg fogunk allni.
 */
@SuppressWarnings("unchecked")
public class MethodHandlingTimeoutTest extends AbstractRedisTest {

    private static final MethodDescriptor METHOD_1 = MethodDescriptor.ofName("GROUP2", "method1");
    private static final ConsumerIdentity consumerIdentity = groupGroup("GROUP2");
    private static final ConsumerIdentity producerIdentity = groupGroup("PRODUCER");

    /**
     * A producer szinkron üzenetet küld, amit senki sem dolgoz fel az adott (100ms) timeouton belül. A visszaadott future cancelled állapotba kerül.
     */
    @Test
    public void testSingleMessageTimeout() throws InterruptedException, SerializationException {
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);
        producer.start();

        CompletableFuture<Map> response = producer.syncCallMethod(METHOD_1.withTimeout(100), singletonMap("aa", 11));

        await("Response should get cancelled").atMost(1, TimeUnit.SECONDS).until(response::isCancelled);
        producer.stop();
        executor.shutdown();
        await("Executor should terminate").atMost(1, TimeUnit.SECONDS).until(executor::isTerminated);
    }

    /**
     * Több timeoutoló üzenet helyes viselkedését vizsgáljuk. Mindegyik csak a megfelelő időben válik cancelleddé.
     *
     * Harom metodushivas tortenik harom kulonbozo timeout alatt. Egyikre sem valaszol senki.
     */
    @Test
    public void testMultipleMessageTimeout() throws InterruptedException, SerializationException {
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);
        producer.start();

        CompletableFuture<Map> response = producer.syncCallMethod(METHOD_1.withTimeout(100), singletonMap("aa", 11));
        CompletableFuture<Map> response2 = producer.syncCallMethod(METHOD_1.withTimeout(2000), singletonMap("aa", 11));
        CompletableFuture<Map> response3 = producer.syncCallMethod(METHOD_1.withTimeout(4000), singletonMap("aa", 11));

        await("First response should cancel in 100ms").atMost(200, TimeUnit.MILLISECONDS).until(response::isCancelled);
        await("Response 2 should not cancel too early").during(1400, TimeUnit.MILLISECONDS).until(() -> !response2.isCancelled());
        await("Response 2 should cancel").atMost(600, TimeUnit.MILLISECONDS).until(response2::isCancelled);
        await("Response 3 should not cancel too early").during(1500, TimeUnit.MILLISECONDS).until(() -> !response3.isCancelled());
        await("Response 3 should cancel").atMost(600, TimeUnit.MILLISECONDS).until(response2::isCancelled);

        producer.stop();
        executor.shutdown();
        await("Executor should terminate").atMost(1, TimeUnit.SECONDS).until(executor::isTerminated);
    }

    /**
     * A timeoutolást nézi egy realisztikusabb scenárióban, ahol van consumer is.
     *
     * Itt a consumer megkapja a kerest es elkezdi feldolgozni, de mar csak boven a timeout utan irna vissza a valaszt.
     * Ezert a hivo oldalon CancellationException-t dob a .get() hivas.
     */
    @Test
    public void testGettingCancellationException() throws InterruptedException, SerializationException, ExecutionException, TimeoutException {
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), consumerIdentity, executor);

        final AtomicBoolean startedProcessing = new AtomicBoolean(false);
        consumer.registerMethodProcessor(SelfDescribingMethodProcessor.of(METHOD_1, x -> {
            try {
                startedProcessing.set(true);
                Thread.sleep(1000);
                return singletonMap("a", 1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));
        consumer.start();

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);
        producer.start();

        // itt csak megvarjuk, amig elindul a valaszolo fel.
        await("Counsumer should start processing").atMost(1, TimeUnit.SECONDS).until(consumer::isRunning);

        CompletableFuture<Map> response = producer.syncCallMethod(METHOD_1.withTimeout(100), singletonMap("aa", 11));

        try {
            // mivel
            response.get(3, TimeUnit.SECONDS);
            fail("Exception should have been thrown!");
        } catch (CancellationException ignore) {
            // expected
        }

        assertTrue(startedProcessing.get());

        producer.stop();
        consumer.stop();
        executor.shutdown();
        await("Executor should terminate").atMost(3, TimeUnit.SECONDS).until(executor::isTerminated);
    }


    /**
     * Itt azt a működést várjuk el, hogy ha a consumer válaszol a timeout előtt, de a csak a timeout után olvassuk ki a választ, akkor ne kapjunk timeout exception-t
     */
    @Test
    public void testNoExceptionIfReadingLater() throws InterruptedException, SerializationException, ExecutionException, TimeoutException {
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), consumerIdentity, executor);

        consumer.registerMethodProcessor(SelfDescribingMethodProcessor.of(METHOD_1, x -> singletonMap("a", 1)));
        consumer.start();

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);
        producer.start();

        await("Counsumer should start processing").atMost(1, TimeUnit.SECONDS).until(consumer::isRunning);
        CompletableFuture<Map> response = producer.syncCallMethod(METHOD_1.withTimeout(500), singletonMap("aa", 11));

        response.get(3, TimeUnit.SECONDS);

        producer.stop();
        consumer.stop();
        executor.shutdown();
        await("Executor should terminate").atMost(3, TimeUnit.SECONDS).until(executor::isTerminated);

        assertEquals(singletonMap("a", 1), response.get());
    }
}