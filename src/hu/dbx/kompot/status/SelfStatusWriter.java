package hu.dbx.kompot.status;

import hu.dbx.kompot.core.KeyNaming;
import hu.dbx.kompot.impl.consumer.ConsumerConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Transaction;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Beirja sajat magat a
 */
public class SelfStatusWriter implements Runnable {

    public static final long TIMEOUT_IN_SECS = 60L * 5L;

    private final ConsumerConfig consumerConfig;

    private SelfStatusWriter(ConsumerConfig consumerConfig) {
        this.consumerConfig = consumerConfig;
    }

    /**
     * Writes status under key with timeout.
     */
    @Override
    public void run() {
        final String key = heartbeatKey();
        final String time = new Date().toString();

        try (final Jedis jedis = consumerConfig.getPool().getResource()) {
            final Transaction tr = jedis.multi();
            tr.persist(key); // remove timeout now
            tr.expire(key, (int) TIMEOUT_IN_SECS);
            tr.hset(key, "time", time); // TODO: create some date utility.
            tr.exec();
        }
    }

    /**
     * Deletes its heartbeat key
     */
    public static void delete(ConsumerConfig config) {
        final SelfStatusWriter writer = new SelfStatusWriter(config);
        final String key = writer.heartbeatKey();

        try (final Jedis jedis = config.getPool().getResource()) {
            jedis.del(key); // remove timeout now
        }
    }

    private String heartbeatKey() {
        final String group = consumerConfig.getConsumerIdentity().getEventGroup();
        final String id = consumerConfig.getConsumerIdentity().getIdentifier();
        return consumerConfig.getNaming().statusHolderKey(group, id);
    }

    /**
     * Returns a set of all module status keys.
     */
    static Set<String> findStatusKeys(Jedis jedis, KeyNaming keyNaming) {
        final Set<String> statusKeys = new HashSet<>();
        final String pattern = keyNaming.statusHolderKey("*", "*");

        ScanResult<String> result;
        String cursor = "0";

        do {
            result = jedis.scan(cursor, new ScanParams().match(pattern).count(256));
            cursor = result.getCursor();

            statusKeys.addAll(result.getResult());
        } while (!result.isCompleteIteration());

        return statusKeys;
    }

    /**
     * Elinditja az idozitott utemezot, ami rendszeresen beirja a sajat statuszt redis-be.
     */
    public static void start(ConsumerConfig consumerConfig) {
        final SelfStatusWriter writer = new SelfStatusWriter(consumerConfig);
        consumerConfig.getScheduledExecutor().scheduleAtFixedRate(writer, 0L, TIMEOUT_IN_SECS, TimeUnit.SECONDS);
    }
}
