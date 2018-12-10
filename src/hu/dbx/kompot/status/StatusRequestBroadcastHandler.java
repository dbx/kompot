package hu.dbx.kompot.status;

import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.broadcast.handler.SelfDescribingBroadcastProcessor;
import hu.dbx.kompot.core.SerializeHelper;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.LoggerUtils;
import hu.dbx.kompot.impl.consumer.ConsumerConfig;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Every module is subscribed to this broadcast by default.
 * <p>
 * When received the module writes its status to the response key found in payload.
 */
public class StatusRequestBroadcastHandler implements SelfDescribingBroadcastProcessor<Map> {

    private static final Logger LOGGER = LoggerUtils.getLogger();

    // @SuppressWarnings("unchecked")
    private static final BroadcastDescriptor<Map> DESCRIPTOR = BroadcastDescriptor.of("KMPT_SAY_HELLO", Map.class);

    private final Supplier<StatusReport> statusReportFactory;
    private final ConsumerConfig config;

    public StatusRequestBroadcastHandler(Supplier<StatusReport> statusReportFactory, ConsumerConfig config) {
        this.statusReportFactory = statusReportFactory;
        this.config = config;
    }

    @Override
    public BroadcastDescriptor<Map> getBroadcastDescriptor() {
        return DESCRIPTOR;
    }

    @Override
    public void handle(Map request) {
        final String responseKey = (String) request.get("key");
        LOGGER.debug("Writing status to key {}", responseKey);

        assert (responseKey != null);

        final StatusReport status = statusReportFactory.get();

        try (final Jedis jedis = config.getPool().getResource()) {
            final String serialized = SerializeHelper.serializeObject(status);

            jedis.rpush(responseKey, serialized);
        } catch (SerializationException e) {
            throw new RuntimeException(e);
        }

        LOGGER.debug("Successfully written status to key {}", responseKey);
    }
}
