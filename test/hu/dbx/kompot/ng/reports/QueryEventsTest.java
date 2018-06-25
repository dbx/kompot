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
import static org.junit.Assert.assertTrue;

public class QueryEventsTest {

    private static final Logger LOGGER = LoggerUtils.getLogger();

    private static final EventDescriptor<Map> EVENT_1 = EventDescriptor.of("EVENT2", Map.class);
    private static final ConsumerIdentity consumerIdentity = groupGroup("EVENT2");
    private static final ConsumerIdentity producerIdentity = groupGroup("EVENTP");


    @Rule
    public TestRedis redis = TestRedis.build();

    @Test
    public void testEventsQuery() throws SerializationException, InterruptedException {

        final ExecutorService executor = Executors.newFixedThreadPool(4);
        //TODO: ezt a DefaultKeyNaming.ofPrefix-et nem itt kellene hívni, hanem legalábbis a CommunicationEndpoint-tól lekérni
        final Reporting reporting = Reporting.ofRedisConnectionUri(redis.getConnectionURI(), DefaultKeyNaming.ofPrefix("moby"));

        //db takarítás
        try (Jedis jedis = redis.getJedisPool().getResource()) {
            jedis.flushDB();
        }

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);
        producer.start();

        for (int i = 0; i < 10; i++)
            producer.asyncSendEvent(EVENT_1, singletonMap("index", i));

        {
            //feldolgozás előtt álló események
            final ListResult<UUID> uuids = reporting.queryEventUuids("EVENT2", EventFilters.forStatus(DataHandling.Statuses.CREATED), Pagination.fromOffsetAndLimit(0, 1000));
            assertEquals(10, uuids.getTotal());
        }

        final CommunicationEndpoint consumer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.empty(), consumerIdentity, executor);
        consumer.registerEventHandler(SelfDescribingEventProcessor.of(EVENT_1, (data, callback) -> {

            {
                final ListResult<UUID> uuids = reporting.queryEventUuids("EVENT2",
                        EventFilters.forStatus(DataHandling.Statuses.PROCESSING),
                        Pagination.fromOffsetAndLimit(0, 1000));
                assertTrue(uuids.getTotal() >= 1);
            }

            final int index = (int) data.get("index");

            //az események felét hibaként eldobjuk, hogy azt az ágat is lehessen tesztelni
            if (index % 2 == 1) {
                callback.success("OK! :)");
            } else {
                callback.error("Not ok :(");
            }
        }));
        consumer.start();

        Thread.sleep(1000);

        {
            final ListResult<UUID> uuids = reporting.queryEventUuids("EVENT2",
                    EventFilters.forStatus(DataHandling.Statuses.PROCESSED),
                    Pagination.fromOffsetAndLimit(0, 1000));
            assertEquals(5, uuids.getTotal());
        }

        {
            final ListResult<UUID> uuids = reporting.queryEventUuids("EVENT2",
                    EventFilters.forStatus(DataHandling.Statuses.ERROR),
                    Pagination.fromOffsetAndLimit(0, 1000));
            assertEquals(5, uuids.getTotal());
        }
    }

}
