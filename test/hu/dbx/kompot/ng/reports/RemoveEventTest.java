package hu.dbx.kompot.ng.reports;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.TestRedis;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.EventFrame;
import hu.dbx.kompot.consumer.async.EventSendingCallback;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.DataHandling;
import hu.dbx.kompot.impl.DefaultKeyNaming;
import hu.dbx.kompot.producer.EventGroupProvider;
import hu.dbx.kompot.report.Reporting;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static java.util.Collections.singletonMap;

public class RemoveEventTest {

    private static final EventDescriptor<Map> EVENT_1 = EventDescriptor.of("EVENT3", Map.class);
    private static final ConsumerIdentity producerIdentity = groupGroup("EVENTP");

    @Rule
    public TestRedis redis = TestRedis.build();

    @Before
    public void before() {
        try (Jedis jedis = redis.getJedisPool().getResource()) {
            jedis.flushDB();
        }
    }

    @Test
    public void removeEvent() throws SerializationException {

        ExecutorService executor = Executors.newFixedThreadPool(4);
        AtomicReference<UUID> sentEventUuid = new AtomicReference<>(null);

        //TODO: ezt a DefaultKeyNaming.ofPrefix-et nem itt kellene hívni, hanem legalábbis a CommunicationEndpoint-tól lekérni
        Reporting reporting = Reporting.ofRedisConnectionUri(redis.getConnectionURI(), DefaultKeyNaming.ofPrefix("moby"));

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

        Assert.assertEquals(DataHandling.Statuses.CREATED, reporting.querySingleEvent("EVENT3", sentEventUuid.get()).get().getStatus());

        reporting.removeEvent(sentEventUuid.get(), "EVENT3");

        Assert.assertFalse(reporting.querySingleEvent("EVENT3", sentEventUuid.get()).isPresent());
    }
}
