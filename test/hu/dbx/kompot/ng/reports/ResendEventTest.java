package hu.dbx.kompot.ng.reports;


import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.EventFrame;
import hu.dbx.kompot.consumer.async.EventSendingCallback;
import hu.dbx.kompot.consumer.async.handler.SelfDescribingEventProcessor;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.DataHandling;
import hu.dbx.kompot.impl.DefaultKeyNaming;
import hu.dbx.kompot.impl.LoggerUtils;
import hu.dbx.kompot.ng.AbstractRedisTest;
import hu.dbx.kompot.producer.EventGroupProvider;
import hu.dbx.kompot.report.EventGroupData;
import hu.dbx.kompot.report.Reporting;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.*;

@SuppressWarnings("ConstantConditions")
public class ResendEventTest extends AbstractRedisTest {

    private static final Logger LOGGER = LoggerUtils.getLogger();

    private static final String CONSUMER_CODE = "CONS_RESEND_EVENT_TEST";
    private static final EventDescriptor<String> EVENT_1 = EventDescriptor.of(CONSUMER_CODE, String.class);
    private static final ConsumerIdentity consumerIdentity = groupGroup(CONSUMER_CODE);
    private static final ConsumerIdentity producerIdentity = groupGroup("EVENTP");

    @Test
    public void invalidResendStateUnprocessedTest() throws SerializationException {

        TestInit testInit = new TestInit().invoke();
        UUID sentEventUuid = testInit.getSentEventUuid();
        Reporting reporting = testInit.getReporting();

        final Optional<EventGroupData> event = reporting.querySingleEvent(CONSUMER_CODE, sentEventUuid);
        assertTrue("Event should be present", event.isPresent());
        assertEquals(DataHandling.Statuses.CREATED, event.get().getStatus());

        assertThrows(IllegalArgumentException.class, () -> reporting.resendEvent(sentEventUuid, CONSUMER_CODE));
    }

    @Test
    public void invalidResendStateProcessedTest() throws SerializationException {

        TestInit testInit = new TestInit().invoke();
        UUID sentEventUuid = testInit.getSentEventUuid();
        Reporting reporting = testInit.getReporting();

        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.empty(), consumerIdentity, testInit.getExecutor());
        consumer.registerEventHandler(SelfDescribingEventProcessor.of(EVENT_1, (data, meta, callback) -> callback.success(":)")));

        consumer.start();
        await("Consumer should run at least 500 ms").during(500, TimeUnit.MILLISECONDS).atMost(1, TimeUnit.SECONDS).until(consumer::isRunning);
        consumer.stop();

        final Optional<EventGroupData> event = reporting.querySingleEvent(CONSUMER_CODE, sentEventUuid);
        assertTrue("Event should be present", event.isPresent());
        assertEquals(DataHandling.Statuses.PROCESSED, event.get().getStatus());

        assertThrows(IllegalArgumentException.class, () -> reporting.resendEvent(sentEventUuid, CONSUMER_CODE));
    }

    @Test
    public void resendProcessingEvent() throws SerializationException {

        TestInit testInit = new TestInit().invoke();
        UUID sentEventUuid = testInit.getSentEventUuid();
        Reporting reporting = testInit.getReporting();

        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.empty(), consumerIdentity, testInit.getExecutor());
        consumer.registerEventHandler(SelfDescribingEventProcessor.of(EVENT_1, (data, meta, callback) -> LOGGER.info("Nem csinálok semmit!")));

        consumer.start();
        await("Consumer should run at least 500 ms").during(500, TimeUnit.MILLISECONDS).atMost(1, TimeUnit.SECONDS).until(consumer::isRunning);
        consumer.stop();

        Optional<EventGroupData> event = reporting.querySingleEvent(CONSUMER_CODE, sentEventUuid);
        assertTrue("Event should be present", event.isPresent());
        assertEquals(DataHandling.Statuses.PROCESSING, event.get().getStatus());

        reporting.resendEvent(sentEventUuid, CONSUMER_CODE);

        event = reporting.querySingleEvent(CONSUMER_CODE, sentEventUuid);
        assertTrue("Event should be present", event.isPresent());
        assertEquals(DataHandling.Statuses.CREATED, event.get().getStatus());
    }

    @Test
    public void resendErroneousEvent() throws SerializationException {

        TestInit testInit = new TestInit().invoke();
        UUID sentEventUuid = testInit.getSentEventUuid();
        Reporting reporting = testInit.getReporting();

        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.empty(), consumerIdentity, testInit.getExecutor());
        consumer.registerEventHandler(SelfDescribingEventProcessor.of(EVENT_1, (data, meta, callback) -> callback.error(":'(")));

        consumer.start();
        await("Consumer should run at least 500 ms").during(500, TimeUnit.MILLISECONDS).atMost(1, TimeUnit.SECONDS).until(consumer::isRunning);
        consumer.stop();

        Optional<EventGroupData> event = reporting.querySingleEvent(CONSUMER_CODE, sentEventUuid);
        assertTrue("Event should be present", event.isPresent());
        assertEquals(DataHandling.Statuses.ERROR, event.get().getStatus());

        reporting.resendEvent(sentEventUuid, CONSUMER_CODE);

        event = reporting.querySingleEvent(CONSUMER_CODE, sentEventUuid);
        assertTrue("Event should be present", event.isPresent());
        assertEquals(DataHandling.Statuses.CREATED, event.get().getStatus());
    }

    private static class TestInit {
        private ExecutorService executor;
        private final AtomicReference<UUID> sentEventUuid = new AtomicReference<>();
        private Reporting reporting;

        private ExecutorService getExecutor() {
            return executor;
        }

        private UUID getSentEventUuid() {
            return sentEventUuid.get();
        }

        private Reporting getReporting() {
            return reporting;
        }

        private TestInit invoke() throws SerializationException {
            executor = Executors.newFixedThreadPool(4);
            sentEventUuid.set(null);

            //TODO: ezt a DefaultKeyNaming.ofPrefix-et nem itt kellene hívni, hanem legalábbis a CommunicationEndpoint-tól lekérni
            reporting = Reporting.ofRedisConnectionUri(redis.getConnectionURI(), DefaultKeyNaming.ofPrefix("moby"));

            final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);
            producer.registerEventSendingCallback(new EventSendingCallback() {
                @Override
                public void onEventSent(EventFrame frame) {
                    sentEventUuid.set(frame.getIdentifier());
                }

                @Override
                public void beforeEventSent(EventFrame frame) {

                }
            });
            producer.start();
            producer.asyncSendEvent(EVENT_1, "aa");
            producer.stop();
            return this;
        }
    }
}
