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
import hu.dbx.kompot.ng.AbstractRedisTest;
import hu.dbx.kompot.producer.EventGroupProvider;
import hu.dbx.kompot.report.EventData;
import hu.dbx.kompot.report.EventGroupData;
import hu.dbx.kompot.report.Reporting;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.*;

public class QuerySingleEventTest extends AbstractRedisTest {

    private static final String EVENT_NAME = "TEST_EVENT_" + QuerySingleEventTest.class.getName();

    private static final EventDescriptor<String> EVENT_1 = EventDescriptor.of(EVENT_NAME, String.class);
    private static final ConsumerIdentity consumerIdentity = groupGroup(EVENT_NAME);
    private static final ConsumerIdentity producerIdentity = groupGroup("EVENTP");

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
        producer.asyncSendEvent(EVENT_1, "aa");
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
    public void querySingleEventLifecycle() throws SerializationException {
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
        producer.asyncSendEvent(EVENT_1, "aa");
        producer.stop();

        final UUID sentUUID = await("Sent event uuid should be set").atMost(1, TimeUnit.SECONDS).untilAtomic(sentEventUuid, Matchers.notNullValue());

        {
            //az esemény feldolgozás előtt van
            final Optional<EventGroupData> eventGroupDataOpt = reporting.querySingleEvent(EVENT_1.getEventName(), sentUUID);
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

        await("Consumer should run for at least half a second").during(500, TimeUnit.MILLISECONDS).until(consumer::isRunning);

        {
            //az esemény feldolgozás után van
            final Optional<EventGroupData> eventGroupDataOpt = reporting.querySingleEvent(EVENT_1.getEventName(), sentEventUuid.get());
            assertTrue(eventGroupDataOpt.isPresent());
            assertEquals(DataHandling.Statuses.PROCESSED, eventGroupDataOpt.get().getStatus());
        }

        {
            //az esemény feldolgozás után van
            final Optional<EventGroupData> eventGroupDataOpt = reporting.querySingleEvent(EVENT_1.getEventName(), sentEventUuid.get());
            assertTrue(eventGroupDataOpt.isPresent());
            assertEquals(DataHandling.Statuses.PROCESSED, eventGroupDataOpt.get().getStatus());
        }
        consumer.stop();
        executor.shutdown();
        await("Executor should terminate").atMost(1, TimeUnit.SECONDS).until(executor::isTerminated);
    }
}
