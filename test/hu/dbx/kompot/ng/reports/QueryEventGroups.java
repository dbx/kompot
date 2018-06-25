package hu.dbx.kompot.ng.reports;


import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.TestRedis;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.DefaultKeyNaming;
import hu.dbx.kompot.impl.LoggerUtils;
import hu.dbx.kompot.producer.EventGroupProvider;
import hu.dbx.kompot.report.Reporting;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryEventGroups {

    private static final Logger LOGGER = LoggerUtils.getLogger();

    private static final EventDescriptor<Map> EVENT_1 = EventDescriptor.of("EVENT1", Map.class);
    private static final EventDescriptor<Map> EVENT_2 = EventDescriptor.of("EVENT2", Map.class);
    private static final ConsumerIdentity producerIdentity = groupGroup("PROD");


    @Rule
    public TestRedis redis = TestRedis.build();

    @Before
    public void before() {
        try (Jedis jedis = redis.getJedisPool().getResource()) {
            jedis.flushDB();
        }
    }

    @Test
    public void test() throws SerializationException {
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        //TODO: ezt a DefaultKeyNaming.ofPrefix-et nem itt kellene hívni, hanem legalábbis a CommunicationEndpoint-tól lekérni
        final Reporting reporting = Reporting.ofRedisConnectionUri(redis.getConnectionURI(), DefaultKeyNaming.ofPrefix("moby"));

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);
        producer.start();
        producer.asyncSendEvent(EVENT_1, singletonMap("aa", 0));

        assertEquals(1, reporting.listAllEventGroups().size());

        producer.asyncSendEvent(EVENT_2, singletonMap("bb", 1));

        assertEquals(2, reporting.listAllEventGroups().size());


        //TODO: itt miért event neveket látok csoportok helyett?

        final Set<String> groups = new HashSet<>(reporting.listAllEventGroups());

        assertTrue(groups.containsAll(Arrays.asList("EVENT1", "EVENT2")));
    }

}
