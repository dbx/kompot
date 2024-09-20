package hu.dbx.kompot.impl.jedis.status;

import hu.dbx.kompot.core.SerializeHelper;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.impl.consumer.ConsumerConfig;
import hu.dbx.kompot.impl.jedis.JedisMessagingService;
import hu.dbx.kompot.status.StatusReport;
import hu.dbx.kompot.status.StatusRequestBroadcastHandler;
import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.function.Supplier;

public class JedisStatusRequestBroadcastHandler extends StatusRequestBroadcastHandler {

    public JedisStatusRequestBroadcastHandler(Supplier<StatusReport> statusReportFactory, ConsumerConfig config) {
        super(statusReportFactory, config);
    }

    @Override
    public void handle(Map request) {
        final String responseKey = (String) request.get("key");
        LOGGER.debug("Writing status to key {}", responseKey);

        assert (responseKey != null);

        final StatusReport status = statusReportFactory.get();
        LOGGER.info("Current status is: {}", status);

        final JedisMessagingService messagingService = (JedisMessagingService) config.getMessagingService();

        try (final Jedis jedis = messagingService.getPool().getResource()) {
            final String serialized = SerializeHelper.serializeObject(status);

            jedis.rpush(responseKey, serialized);
        } catch (SerializationException e) {
            throw new RuntimeException(e);
        }

        LOGGER.debug("Successfully written status to key {}", responseKey);
    }

}
