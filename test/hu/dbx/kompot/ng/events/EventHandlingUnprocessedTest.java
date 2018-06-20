package hu.dbx.kompot.ng.events;

import hu.dbx.kompot.CommunicationEndpoint;
import hu.dbx.kompot.TestRedis;
import hu.dbx.kompot.producer.EventGroupProvider;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static hu.dbx.kompot.impl.DefaultConsumerIdentity.groupGroup;
import static java.util.Collections.singletonMap;

/**
 * Elkuldunk 10 eventet, egyik sem lesz feldolgozva, mert nem fut a szerver.
 * De a kliens nem szall el hibaval, hanem bekesen varakozik.
 */
@SuppressWarnings("unchecked")
public class EventHandlingUnprocessedTest {

    private static final EventDescriptor EVENT_1 = EventDescriptor.of("EVENT1", Map.class);
    private static final ConsumerIdentity serverIdentity = groupGroup("XXX");

    @Rule
    public TestRedis redis = TestRedis.build();

    @Test
    public void testEventsAreNotHandled() throws InterruptedException, SerializationException {
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        final CommunicationEndpoint client = CommunicationEndpoint.ofRedisConnectionUri(redis.getConnectionURI(), EventGroupProvider.identity(), serverIdentity, executor);
        client.start();

        for (int i = 0; i < 10; i++) {
            client.asyncSendEvent(EVENT_1, singletonMap("aa", 11));
        }

        executor.awaitTermination(1, TimeUnit.SECONDS);

        client.stop();
    }
}
