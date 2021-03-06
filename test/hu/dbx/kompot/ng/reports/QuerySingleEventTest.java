package hu.dbx.kompot.ng.reports;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.TestRedis;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.EventFrame;
import hu.dbx.kompot.consumer.async.EventSendingCallback;
import hu.dbx.kompot.consumer.async.handler.SelfDescribingEventProcessor;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.DataHandling;
import hu.dbx.kompot.impl.DefaultKeyNaming;
import hu.dbx.kompot.producer.EventGroupProvider;
import hu.dbx.kompot.report.EventData;
import hu.dbx.kompot.report.EventGroupData;
import hu.dbx.kompot.report.Reporting;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.*;

public class QuerySingleEventTest {

    private static final String EVENT_NAME = "TEST_EVENT_" + QuerySingleEventTest.class.getName();

    private static final EventDescriptor<Map> EVENT_1 = EventDescriptor.of(EVENT_NAME, Map.class);
    private static final ConsumerIdentity consumerIdentity = groupGroup(EVENT_NAME);
    private static final ConsumerIdentity producerIdentity = groupGroup("EVENTP");

    @Rule
    public TestRedis redis = TestRedis.build();

    @Before
    public void before() {
        try (Jedis jedis = redis.getJedisPool().getResource()) {
            jedis.flushDB();
        }
    }

    /**
     * Egy konkrét esemény adatainak lekérdezése. Az esemény UUID-ját az eseményküldés callbackből szedjük ki
     */
    @Test
    public void querySingleEvent() throws SerializationException {
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        final UUID[] sentEventUuid = {null};
        //TODO: ezt a DefaultKeyNaming.ofPrefix-et nem itt kellene hívni, hanem legalábbis a CommunicationEndpoint-tól lekérni
        final Reporting reporting = Reporting.ofRedisConnectionUri(redis.getConnectionURI(), DefaultKeyNaming.ofPrefix("moby"));

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);
        producer.registerEventSendingCallback(new EventSendingCallback() {
            @Override
            public void onEventSent(EventFrame frame) {
                sentEventUuid[0] = frame.getIdentifier();
            }

            @Override
            public void beforeEventSent(EventFrame frame) {

            }
        });
        producer.start();
        producer.asyncSendEvent(EVENT_1, singletonMap("aa", 0));
        producer.stop();

        assertNotNull(sentEventUuid[0]);

        //db takarítás
//        redis.getJedisPool().getResource().flushDB();

        final Optional<EventGroupData> eventGroupDataOpt = reporting.querySingleEvent(EVENT_1.getEventName(), sentEventUuid[0]);

        assertTrue(eventGroupDataOpt.isPresent());
        assertEquals(DataHandling.Statuses.CREATED, eventGroupDataOpt.get().getStatus());

        final EventData eventData = eventGroupDataOpt.get().getEventData();
        assertNotNull(eventData.getData());
        assertNotNull(eventData.getFirstSent());
        assertNotNull(eventData.getSender());
        assertNotNull(eventData.getUuid());
        assertEquals(EVENT_1.getEventName(), eventData.getEventType());
        assertEquals(EVENT_1.getEventName(), eventData.getGroups());
    }

    /**
     * Egy konkrét esemény életciklusának végigjátszása
     */
    @Test
    public void querySingleEventLifecycle() throws SerializationException, InterruptedException {
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        final AtomicReference<UUID> sentEventUuid = new AtomicReference<>();

        //TODO: ezt a DefaultKeyNaming.ofPrefix-et nem itt kellene hívni, hanem legalábbis a CommunicationEndpoint-tól lekérni
        final Reporting reporting = Reporting.ofRedisConnectionUri(redis.getConnectionURI(), DefaultKeyNaming.ofPrefix("moby"));

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
        producer.asyncSendEvent(EVENT_1, singletonMap("aa", 0));
        producer.stop();

        Thread.sleep(100);
        assertNotNull(sentEventUuid.get());

        {
            //az esemény feldolgozás előtt van
            final Optional<EventGroupData> eventGroupDataOpt = reporting.querySingleEvent(EVENT_1.getEventName(), sentEventUuid.get());
            assertTrue(eventGroupDataOpt.isPresent());
            assertEquals(DataHandling.Statuses.CREATED, eventGroupDataOpt.get().getStatus());
        }

        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.empty(), consumerIdentity, executor);
        consumer.registerEventHandler(SelfDescribingEventProcessor.of(EVENT_1, (data, meta, callback) -> {

            //az esemény feldolgozás alatt van
            final Optional<EventGroupData> eventGroupDataOpt = reporting.querySingleEvent(EVENT_1.getEventName(), sentEventUuid.get());
            assertTrue(eventGroupDataOpt.isPresent());
            assertEquals(DataHandling.Statuses.PROCESSING, eventGroupDataOpt.get().getStatus());

            callback.success("OK");
        }));
        consumer.start();

        Thread.sleep(500);

        {
            //az esemény feldolgozás után van
            final Optional<EventGroupData> eventGroupDataOpt = reporting.querySingleEvent(EVENT_1.getEventName(), sentEventUuid.get());
            assertTrue(eventGroupDataOpt.isPresent());
            assertEquals(DataHandling.Statuses.PROCESSED, eventGroupDataOpt.get().getStatus());
        }
        consumer.stop();
    }
}
