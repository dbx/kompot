package hu.dbx.kompot.ng.reports;


import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.TestRedis;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.handler.SelfDescribingEventProcessor;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.DataHandling;
import hu.dbx.kompot.impl.DefaultKeyNaming;
import hu.dbx.kompot.impl.LoggerUtils;
import hu.dbx.kompot.producer.EventGroupProvider;
import hu.dbx.kompot.report.Reporting;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static java.util.Collections.singletonMap;

@SuppressWarnings("ConstantConditions")
public class ResendEventTest {

    private static final Logger LOGGER = LoggerUtils.getLogger();

    private static final EventDescriptor<Map> EVENT_1 = EventDescriptor.of("EVENT3", Map.class);
    private static final ConsumerIdentity consumerIdentity = groupGroup("EVENT3");
    private static final ConsumerIdentity producerIdentity = groupGroup("EVENTP");

    @Rule
    public TestRedis redis = TestRedis.build();

    @Before
    public void before() {
        try (Jedis jedis = redis.getJedisPool().getResource()) {
            jedis.flushDB();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidResendStateUnprocessedTest() throws SerializationException {

        TestInit testInit = new TestInit().invoke();
        UUID[] sentEventUuid = testInit.getSentEventUuid();
        Reporting reporting = testInit.getReporting();

        Assert.assertEquals(DataHandling.Statuses.CREATED, reporting.querySingleEvent("EVENT3", sentEventUuid[0]).get().getStatus());

        reporting.resendEvent(sentEventUuid[0], "EVENT3");
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidResendStateProcessedTest() throws SerializationException, InterruptedException {

        TestInit testInit = new TestInit().invoke();
        UUID[] sentEventUuid = testInit.getSentEventUuid();
        Reporting reporting = testInit.getReporting();

        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.empty(), consumerIdentity, testInit.getExecutor());
        consumer.registerEventHandler(SelfDescribingEventProcessor.of(EVENT_1, (data, meta, callback) -> callback.success(":)")));

        consumer.start();
        Thread.sleep(500);
        consumer.stop();

        Assert.assertEquals(DataHandling.Statuses.PROCESSED, reporting.querySingleEvent("EVENT3", sentEventUuid[0]).get().getStatus());

        reporting.resendEvent(sentEventUuid[0], "EVENT3");
    }

    @Test
    public void resendProcessingEvent() throws SerializationException, InterruptedException {

        TestInit testInit = new TestInit().invoke();
        UUID[] sentEventUuid = testInit.getSentEventUuid();
        Reporting reporting = testInit.getReporting();

        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.empty(), consumerIdentity, testInit.getExecutor());
        consumer.registerEventHandler(SelfDescribingEventProcessor.of(EVENT_1, (data, meta, callback) -> LOGGER.info("Nem csinálok semmit!")));

        consumer.start();
        Thread.sleep(500);
        consumer.stop();

        Assert.assertEquals(DataHandling.Statuses.PROCESSING, reporting.querySingleEvent("EVENT3", sentEventUuid[0]).get().getStatus());

        reporting.resendEvent(sentEventUuid[0], "EVENT3");

        Assert.assertEquals(DataHandling.Statuses.CREATED, reporting.querySingleEvent("EVENT3", sentEventUuid[0]).get().getStatus());
    }

    @Test
    public void resendErroneousEvent() throws SerializationException, InterruptedException {

        TestInit testInit = new TestInit().invoke();
        UUID[] sentEventUuid = testInit.getSentEventUuid();
        Reporting reporting = testInit.getReporting();

        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.empty(), consumerIdentity, testInit.getExecutor());
        consumer.registerEventHandler(SelfDescribingEventProcessor.of(EVENT_1, (data, meta, callback) -> callback.error(":'(")));

        consumer.start();
        Thread.sleep(500);
        consumer.stop();

        Assert.assertEquals(DataHandling.Statuses.ERROR, reporting.querySingleEvent("EVENT3", sentEventUuid[0]).get().getStatus());

        reporting.resendEvent(sentEventUuid[0], "EVENT3");

        Assert.assertEquals(DataHandling.Statuses.CREATED, reporting.querySingleEvent("EVENT3", sentEventUuid[0]).get().getStatus());
    }

    private class TestInit {
        private ExecutorService executor;
        private UUID[] sentEventUuid;
        private Reporting reporting;

        private ExecutorService getExecutor() {
            return executor;
        }

        private UUID[] getSentEventUuid() {
            return sentEventUuid;
        }

        private Reporting getReporting() {
            return reporting;
        }

        private TestInit invoke() throws SerializationException {
            executor = Executors.newFixedThreadPool(4);
            sentEventUuid = new UUID[]{null};

            //TODO: ezt a DefaultKeyNaming.ofPrefix-et nem itt kellene hívni, hanem legalábbis a CommunicationEndpoint-tól lekérni
            reporting = Reporting.ofRedisConnectionUri(redis.getConnectionURI(), DefaultKeyNaming.ofPrefix("moby"));

            final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);
            producer.registerEventSendingCallback(frame -> sentEventUuid[0] = frame.getIdentifier());
            producer.start();
            producer.asyncSendEvent(EVENT_1, singletonMap("aa", 0));

            return this;
        }
    }
}
