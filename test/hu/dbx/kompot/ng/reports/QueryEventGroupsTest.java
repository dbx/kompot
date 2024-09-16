package hu.dbx.kompot.ng.reports;


import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.jedis.DefaultKeyNaming;
import hu.dbx.kompot.ng.AbstractRedisTest;
import hu.dbx.kompot.producer.EventGroupProvider;
import hu.dbx.kompot.report.Reporting;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryEventGroupsTest extends AbstractRedisTest {

    private static final EventDescriptor<String> EVENT_1 = EventDescriptor.of("EVENT1", String.class);
    private static final EventDescriptor<String> EVENT_2 = EventDescriptor.of("EVENT2", String.class);
    private static final ConsumerIdentity producerIdentity = groupGroup("PROD");

    @Test
    public void queryEventGroups() throws SerializationException {
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        //TODO: ezt a DefaultKeyNaming.ofPrefix-et nem itt kellene hívni, hanem legalábbis a CommunicationEndpoint-tól lekérni
        final Reporting reporting = Reporting.ofRedisConnectionUri(redis.getConnectionURI(), DefaultKeyNaming.ofPrefix("moby"));

        final CommunicationEndpoint producer = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), producerIdentity, executor);
        producer.start();
        producer.asyncSendEvent(EVENT_1, "aa");

        assertEquals(1, reporting.listAllEventGroups().size());

        producer.asyncSendEvent(EVENT_2, "bb");

        producer.stop();

        assertEquals(2, reporting.listAllEventGroups().size());

        //TODO: itt miért event neveket látok csoportok helyett?

        final Set<String> groups = new HashSet<>(reporting.listAllEventGroups());

        assertTrue(groups.containsAll(Arrays.asList("EVENT1", "EVENT2")));
    }

}
