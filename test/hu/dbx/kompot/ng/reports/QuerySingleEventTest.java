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
import hu.dbx.kompot.report.EventData;
import hu.dbx.kompot.report.EventGroupData;
import hu.dbx.kompot.report.Reporting;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.*;

public class QuerySingleEventTest {

    private static final Logger LOGGER = LoggerUtils.getLogger();

    private static final EventDescriptor<Map> EVENT_1 = EventDescriptor.of("EVENT1", Map.class);
    private static final ConsumerIdentity consumerIdentity = groupGroup("EVENT1");
    private static final ConsumerIdentity producerIdentity = groupGroup("EVENTP");


    @Rule
    public TestRedis redis = TestRedis.build();

    @Test
    public void querySingleEvent() throws SerializationException {
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        final UUID[] sentEventUuid = {null};
        //TODO: ezt a DefaultKeyNaming.ofPrefix-et nem itt kellene hívni, hanem legalábbis a CommunicationEndpoint-tól lekérni
        final Reporting reporting = Reporting.ofRedisConnectionUri(redis.getConnectionURI(), DefaultKeyNaming.ofPrefix("moby"));

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);
        producer.registerEventSendingCallback(frame -> sentEventUuid[0] = frame.getIdentifier());
        producer.start();
        producer.asyncSendEvent(EVENT_1, singletonMap("aa", 0));

        assertNotNull(sentEventUuid[0]);

        //db takarítás
//        redis.getJedisPool().getResource().flushDB();

        final Optional<EventGroupData> eventGroupDataOpt = reporting.querySingleEvent("EVENT1", sentEventUuid[0]);

        assertTrue(eventGroupDataOpt.isPresent());
        assertEquals(DataHandling.Statuses.CREATED, eventGroupDataOpt.get().getStatus());

        final EventData eventData = eventGroupDataOpt.get().getEventData();
        assertNotNull(eventData.getData());
        assertNotNull(eventData.getFirstSent());
        assertNotNull(eventData.getSender());
        assertNotNull(eventData.getUuid());
        assertEquals("EVENT1", eventData.getEventType());
        assertEquals("EVENT1", eventData.getGroups());
    }

    @Test
    public void querySingleEventLifecycle() throws SerializationException, InterruptedException {
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        final UUID[] sentEventUuid = {null};

        //TODO: ezt a DefaultKeyNaming.ofPrefix-et nem itt kellene hívni, hanem legalábbis a CommunicationEndpoint-tól lekérni
        final Reporting reporting = Reporting.ofRedisConnectionUri(redis.getConnectionURI(), DefaultKeyNaming.ofPrefix("moby"));

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);
        producer.registerEventSendingCallback(frame -> sentEventUuid[0] = frame.getIdentifier());
        producer.start();
        producer.asyncSendEvent(EVENT_1, singletonMap("aa", 0));

        assertNotNull(sentEventUuid[0]);

        {
            final Optional<EventGroupData> eventGroupDataOpt = reporting.querySingleEvent("EVENT1", sentEventUuid[0]);
            assertTrue(eventGroupDataOpt.isPresent());
            assertEquals(DataHandling.Statuses.CREATED, eventGroupDataOpt.get().getStatus());
        }

        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.empty(), consumerIdentity, executor);
        consumer.registerEventHandler(SelfDescribingEventProcessor.of(EVENT_1, (data, callback) -> {

            final Optional<EventGroupData> eventGroupDataOpt = reporting.querySingleEvent("EVENT1", sentEventUuid[0]);
            assertTrue(eventGroupDataOpt.isPresent());
            assertEquals(DataHandling.Statuses.PROCESSING, eventGroupDataOpt.get().getStatus());

            callback.success("OK");
        }));
        consumer.start();

        Thread.sleep(500);

        {
            final Optional<EventGroupData> eventGroupDataOpt = reporting.querySingleEvent("EVENT1", sentEventUuid[0]);
            assertTrue(eventGroupDataOpt.isPresent());
            assertEquals(DataHandling.Statuses.PROCESSED, eventGroupDataOpt.get().getStatus());
        }
    }

}
