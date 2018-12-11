package hu.dbx.kompot.ng.status;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.TestRedis;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.producer.EventGroupProvider;
import hu.dbx.kompot.status.StatusReport;
import hu.dbx.kompot.status.StatusReporter;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.fromGroups;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static junit.framework.TestCase.*;

/**
 * A tuloldalon kivetel kepzodott.
 */
@SuppressWarnings("unchecked")
public class StatusReportTest {

    private static final ConsumerIdentity consumerIdentity = fromGroups("EGROUP", "MGROUP");


    @Rule
    public TestRedis redis = TestRedis.build();

    @Test
    public void testFindsOwnStatusReporter() throws InterruptedException {
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), consumerIdentity, executor);

        consumer.registerStatusReporter(new StatusReporter("short", "long descr", () -> StatusReporter.StatusResult.success("status_msg")));

        consumer.start();

        // TODO: what if i remove the sleep here: why does it fail?
        Thread.sleep(2000L);
        final List<StatusReport> statuses = consumer.findGlobalStatuses();

        assertEquals(1, statuses.size());


        assertEquals("EGROUP", statuses.get(0).getEventGroup());
        assertEquals("MGROUP", statuses.get(0).getMessageGroup());

        assertEquals(new HashSet<>(singletonList("KMPT_SAY_HELLO")), statuses.get(0).getRegisteredBroadcasts());
        assertEquals(emptySet(), statuses.get(0).getRegisteredEvents());
        assertEquals(emptySet(), statuses.get(0).getRegisteredMethods());

        assertEquals("short", statuses.get(0).getItems().get(0).getName());
        assertTrue(statuses.get(0).getItems().get(0).isOk());
        assertEquals("status_msg", statuses.get(0).getItems().get(0).getStatusMessage());

        // TODO: legyen olyan teszt is, hogy masik modul statuszat is megtalaljuk.

        // TODO: legyen arra is teszt, hogy a statusz gyarto fuggveny betojik egy masik modulnal, ezert az hibasan jon vissza.
        // TODO: legyen teszt arra is, ha timeout van.

        consumer.stop();
        executor.shutdown();
    }

    /**
     * A status item throws an exception but it is still received on the other side.
     */
    @Test
    public void statusItemThrowsExceptionStillReceived() throws InterruptedException {
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), consumerIdentity, executor);

        consumer.registerStatusReporter(new StatusReporter("short", "long descr", () ->
        {
            throw new IllegalStateException("some error");
        }));

        consumer.start();

        // TODO: what if i remove the sleep here: why does it fail?
        Thread.sleep(2000L);
        final List<StatusReport> statuses = consumer.findGlobalStatuses();

        assertEquals(1, statuses.size());


        assertEquals("EGROUP", statuses.get(0).getEventGroup());
        assertEquals("MGROUP", statuses.get(0).getMessageGroup());

        assertEquals(new HashSet<>(singletonList("KMPT_SAY_HELLO")), statuses.get(0).getRegisteredBroadcasts());
        assertEquals(emptySet(), statuses.get(0).getRegisteredEvents());
        assertEquals(emptySet(), statuses.get(0).getRegisteredMethods());

        assertEquals("short", statuses.get(0).getItems().get(0).getName());
        assertFalse(statuses.get(0).getItems().get(0).isOk());
        assertEquals("some error", statuses.get(0).getItems().get(0).getErrorMessage());

        consumer.stop();
        executor.shutdown();
    }
}
