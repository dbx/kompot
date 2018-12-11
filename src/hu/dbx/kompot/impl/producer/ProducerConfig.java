package hu.dbx.kompot.impl.producer;

import hu.dbx.kompot.core.KeyNaming;
import hu.dbx.kompot.producer.ProducerIdentity;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Configuration of a Producer object.
 */
public final class ProducerConfig {

    private final Executor executor;
    private final ScheduledExecutorService scheduledExecutor;
    private final JedisPool pool;
    private final KeyNaming naming;
    private final ProducerIdentity producerIdentity;

    public ProducerConfig(Executor executor, ScheduledExecutorService scheduledExecutor, JedisPool jedisPool, KeyNaming keyNaming, ProducerIdentity producerIdentity) {
        this.executor = executor;
        this.scheduledExecutor = scheduledExecutor;
        this.pool = jedisPool;
        this.naming = keyNaming;
        this.producerIdentity = producerIdentity;
    }

    public Executor getExecutor() {
        return executor;
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

    public ProducerIdentity getProducerIdentity() {
        return producerIdentity;
    }
}
