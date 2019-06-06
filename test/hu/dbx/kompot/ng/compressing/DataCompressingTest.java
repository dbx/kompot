package hu.dbx.kompot.ng.compressing;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.TestRedis;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.consumer.sync.handler.SelfDescribingMethodProcessor;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.LoggerUtils;
import hu.dbx.kompot.producer.EventGroupProvider;
import org.junit.*;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.*;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

public class DataCompressingTest {

    private static final Logger LOGGER = LoggerUtils.getLogger();

    private static final MethodDescriptor METHOD_1 = MethodDescriptor.ofName("CONSUMER", "method1");
    private static final ConsumerIdentity consumerIdentity = groupGroup("CONSUMER");

    private static final ConsumerIdentity producerIdentity = groupGroup("PRODUCER");

    private static final String ROOT_KEY = "root";
    private static final String DATA1 = buildBigData('1');
    private static final String DATA2 = buildBigData('2');

    @Rule
    public TestRedis redis = TestRedis.build();

    private List<Map.Entry<String, Long>> memoryTestResults;

    @Before
    public void init() {
        memoryTestResults = new ArrayList<>();
    }

    @After
    public void printMemoryUsage() {
        memoryTestResults.forEach(p -> LOGGER.debug("{} : {}", p.getKey(), humanReadableByteCount(p.getValue(), true)));
    }


    @Ignore
    @Test
    public void methodTest01() throws InterruptedException, TimeoutException, ExecutionException, SerializationException {
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);

        producer.start();

        final CommunicationEndpoint consumer = startConsumer(executor);

        System.gc();
        registerMemoryUsage("1");

        Thread.sleep(1000);
        @SuppressWarnings("unchecked")
        CompletableFuture<Map> response = producer.syncCallMethod(METHOD_1.withTimeout(100_000), singletonMap(ROOT_KEY, DATA1));

        assertEquals(DATA2, response.get(30, TimeUnit.SECONDS).get(ROOT_KEY));

        registerMemoryUsage("3");
        System.gc();
        registerMemoryUsage("4");

        producer.stop();
        consumer.stop();
        executor.shutdown();
    }

    private CommunicationEndpoint startConsumer(ExecutorService executor) {
        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), consumerIdentity, executor);

        consumer.registerMethodProcessor(SelfDescribingMethodProcessor.of(METHOD_1, (Map<String, Object> x) -> {
            LOGGER.info("Processing method");
            registerMemoryUsage("2");
            assertEquals(DATA1, x.get(ROOT_KEY));
            return singletonMap(ROOT_KEY, DATA2);
        }));

        consumer.start();
        return consumer;
    }

    private void registerMemoryUsage(String key) {
        memoryTestResults.add(new AbstractMap.SimpleEntry<>(key, Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
    }

    private static String buildBigData(char content) {
        // 100 MB
        char[] chars = new char[100_000_000];
        // Optional step - unnecessary if you're happy with the array being full of \0
        Arrays.fill(chars, content);
        return new String(chars);
    }

    //magic from https://stackoverflow.com/questions/3758606/how-to-convert-byte-size-into-human-readable-format-in-java
    private static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

}
