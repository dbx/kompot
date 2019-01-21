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
import hu.dbx.kompot.report.EventFilters;
import hu.dbx.kompot.report.ListResult;
import hu.dbx.kompot.report.Pagination;
import hu.dbx.kompot.report.Reporting;
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
import static org.junit.Assert.assertEquals;

public class ClearProcessedEventsTest {

    private static final Logger LOGGER = LoggerUtils.getLogger();

    private static final String CONSUMER_CODE = "CONS_CLEAR_PROCESSED_EVENTS_TEST";
    private static final EventDescriptor<Map> EVENT_1 = EventDescriptor.of(CONSUMER_CODE, Map.class);
    private static final ConsumerIdentity consumerIdentity = groupGroup(CONSUMER_CODE);
    private static final ConsumerIdentity producerIdentity = groupGroup("EVENTP");

    private static final Pagination THOUSAND = Pagination.fromOffsetAndLimit(0, 1000);

    @Rule
    public TestRedis redis = TestRedis.build();

    @Before
    public void cleanup() {
        try (Jedis jedis = redis.getJedisPool().getResource()) {
            jedis.flushDB();
        }
    }

    /**
     * GIVEN: Küldünk 4 eseményt. Ötöt sikeresen feldolgozunk belőlük, ötöt eldobunk hibával.
     * WHEN: Hívunk rá egy szerver takarítást
     * THEN: Az 2 sikeresen feldolgozott esemény eltűnik
     */
    @Test
    public void testClearProcessed() throws SerializationException, InterruptedException {

        final ExecutorService executor = Executors.newFixedThreadPool(4);
        //TODO: ezt a DefaultKeyNaming.ofPrefix-et nem itt kellene hívni, hanem legalábbis a CommunicationEndpoint-tól lekérni
        final Reporting reporting = Reporting.ofRedisConnectionUri(redis.getConnectionURI(), DefaultKeyNaming.ofPrefix("moby"));

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);
        producer.start();

        final int eventCount = 400;

        for (int i = 0; i < eventCount; i++) {
            producer.asyncSendEvent(EVENT_1, singletonMap("index", i));
        }

        {
            //feldolgozás előtt álló események
            final ListResult<UUID> uuids = reporting.queryEventUuids(CONSUMER_CODE, EventFilters.forStatus(DataHandling.Statuses.CREATED), THOUSAND);
            assertEquals(eventCount, uuids.getTotal());
        }

        producer.stop();

        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.empty(), consumerIdentity, executor);
        consumer.registerEventHandler(SelfDescribingEventProcessor.of(EVENT_1, (data, meta, callback) -> {
            final int index = (int) data.get("index");

            //az események felét hibaként eldobjuk, hogy azt az ágat is lehessen tesztelni
            if (index % 2 == 1) {
                callback.success("OK! :)");
            } else {
                callback.error("Not ok :(");
            }
        }));
        consumer.start();

        Thread.sleep(2000);

        try {
            {
                final ListResult<UUID> uuids = reporting.queryEventUuids(CONSUMER_CODE, EventFilters.forStatus(DataHandling.Statuses.PROCESSED), THOUSAND);
                assertEquals(eventCount / 2, uuids.getTotal());
            }

            {
                final ListResult<UUID> uuids = reporting.queryEventUuids(CONSUMER_CODE, EventFilters.forStatus(DataHandling.Statuses.ERROR), THOUSAND);
                assertEquals(eventCount / 2, uuids.getTotal());
            }

            //WHEN
            reporting.clearCompletedEvents();

            //THEN
            {
                // minden PROCESSED statuszu esemenyt kitakaritottunk!
                final ListResult<UUID> uuids = reporting.queryEventUuids(CONSUMER_CODE,
                        EventFilters.forStatus(DataHandling.Statuses.PROCESSED),
                        THOUSAND);
                assertEquals(0, uuids.getTotal());
            }

            // nem takaritunk ki ERROR statuszu eventeket
            {
                final ListResult<UUID> uuids = reporting.queryEventUuids(CONSUMER_CODE,
                        EventFilters.forStatus(DataHandling.Statuses.ERROR),
                        THOUSAND);
                assertEquals(eventCount / 2, uuids.getTotal());
            }
        } finally {
            consumer.stop();
        }
    }
}
