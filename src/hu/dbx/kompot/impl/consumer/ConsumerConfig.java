package hu.dbx.kompot.impl.consumer;

import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.core.KeyNaming;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Egy consumer rendszerszintu konfiguraciojat tartalmazza.
 */
public final class ConsumerConfig {

    private final Executor executor;
    private final ScheduledExecutorService scheduledExecutor;
    private final ConsumerIdentity identity;
    private final JedisPool pool;
    private final KeyNaming naming;
    private final List<String> logSensitiveDataKeys;

    public ConsumerConfig(Executor executor,
                          ScheduledExecutorService scheduledExecutor,
                          ConsumerIdentity consumerIdentity,
                          JedisPool jedisPool,
                          KeyNaming keyNaming,
                          List<String> logSensitiveDataKeys) {
        this.executor = executor;
        this.scheduledExecutor = scheduledExecutor;
        this.identity = consumerIdentity;
        this.pool = jedisPool;
        this.naming = keyNaming;
        this.logSensitiveDataKeys = logSensitiveDataKeys;
    }

    public Executor getExecutor() {
        return executor;
    }

    public ConsumerIdentity getConsumerIdentity() {
        return identity;
    }

    public JedisPool getPool() {
        return pool;
    }

    public KeyNaming getNaming() {
        return naming;
    }

    public ScheduledExecutorService getScheduledExecutor() {
        return scheduledExecutor;
    }

    public List<String> getLogSensitiveDataKeys() {
        return logSensitiveDataKeys;
    }
}
