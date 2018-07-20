package hu.dbx.kompot.ng.reports;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.TestRedis;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventDescriptor;
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

public class QueryEventsTest {

    private static final EventDescriptor<Map> EVENT_1 = EventDescriptor.of("EVENT2", Map.class);
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

    @Test
    public void testQueryUnprocessed() throws SerializationException, InterruptedException {

        final ExecutorService executor = Executors.newFixedThreadPool(4);
        //TODO: ezt a DefaultKeyNaming.ofPrefix-et nem itt kellene hívni, hanem legalábbis a CommunicationEndpoint-tól lekérni
        final Reporting reporting = Reporting.ofRedisConnectionUri(redis.getConnectionURI(), DefaultKeyNaming.ofPrefix("moby"));

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);
        producer.start();

        for (int i = 0; i < 10; i++) {
            producer.asyncSendEvent(EVENT_1, singletonMap("index", i));
        }

        final ListResult<UUID> uuids = reporting.queryEventUuids("EVENT2", EventFilters.forStatus(DataHandling.Statuses.CREATED), THOUSAND);
        assertEquals(10, uuids.getTotal());

        producer.stop();
    }
}
