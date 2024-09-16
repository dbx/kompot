package hu.dbx.kompot.impl.producer;

import hu.dbx.kompot.core.MessagingService;
import hu.dbx.kompot.producer.ProducerIdentity;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Configuration of a Producer object.
 */
public final class ProducerConfig {

    private final Executor executor;
    private final ScheduledExecutorService scheduledExecutor;
    private final MessagingService messagingService;
    private final ProducerIdentity producerIdentity;

    public ProducerConfig(Executor executor, ScheduledExecutorService scheduledExecutor, MessagingService messagingService, ProducerIdentity producerIdentity) {
        this.executor = executor;
        this.scheduledExecutor = scheduledExecutor;
        this.messagingService = messagingService;
        this.producerIdentity = producerIdentity;
    }

    public Executor getExecutor() {
        return executor;
    }

    public MessagingService getMessagingService() {
        return messagingService;
    }

    public ScheduledExecutorService getScheduledExecutor() {
        return scheduledExecutor;
    }

    public ProducerIdentity getProducerIdentity() {
        return producerIdentity;
    }
}
