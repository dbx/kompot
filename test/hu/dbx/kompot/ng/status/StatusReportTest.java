package hu.dbx.kompot.ng.status;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.TestRedis;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.producer.EventGroupProvider;
import hu.dbx.kompot.status.StatusReport;
import hu.dbx.kompot.status.StatusReporter;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

/**
 * A tuloldalon kivetel kepzodott.
 */
@SuppressWarnings("unchecked")
public class StatusReportTest {

    private static final MethodDescriptor METHOD_1 = MethodDescriptor.ofName("GROUP1", "method1");
    private static final ConsumerIdentity consumerIdentity = groupGroup("GROUP1");
    private static final ConsumerIdentity producerIdentity = groupGroup("GROUP2");

    private static String EXCEPTION_MSG = "Now, I am become Death, the destroyer of worlds";

    @Rule
    public TestRedis redis = TestRedis.build();

    @Test
    public void testFindsOwnStatusReporter() throws InterruptedException {
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), consumerIdentity, executor);

        consumer.registerStatusReporter(new StatusReporter("short", "long descr", StatusReporter.StatusResult::resultOk));

        consumer.start();

        // TODO: what if i remove the sleep here: why does it fail?
        Thread.sleep(2000L);
        final List<StatusReport> statuses = consumer.findGlobalStatuses();

        assertEquals(1, statuses.size());

        assertEquals("short", statuses.get(0).getItems().get(0).getName());
        assertTrue(statuses.get(0).getItems().get(0).isOk());

        // TODO: legyen olyan teszt is, hogy masik modul statuszat is megtalaljuk.

        // TODO: legyen arra is teszt, hogy a statusz gyarto fuggveny betojik egy masik modulnal, ezert az hibasan jon vissza.
        // TODO: legyen teszt arra is, ha timeout van.

        consumer.stop();
        executor.shutdown();
    }

    // TODO: write test case where we find status of other item too.
}
