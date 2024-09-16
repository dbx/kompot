package hu.dbx.kompot.impl.consumer;

import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.core.MessagingService;

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
    private final MessagingService messagingService;
    private final List<String> logSensitiveDataKeys;
    private final int maxEventThreadCount;

    public ConsumerConfig(Executor executor,
                          ScheduledExecutorService scheduledExecutor,
                          ConsumerIdentity consumerIdentity,
                          MessagingService messagingService,
                          List<String> logSensitiveDataKeys,
                          int maxEventThreadCount) {
        this.executor = executor;
        this.scheduledExecutor = scheduledExecutor;
        this.identity = consumerIdentity;
        this.messagingService = messagingService;
        this.logSensitiveDataKeys = logSensitiveDataKeys;
        this.maxEventThreadCount = maxEventThreadCount;
    }

    public Executor getExecutor() {
        return executor;
    }

    public ConsumerIdentity getConsumerIdentity() {
        return identity;
    }

    public MessagingService getMessagingService() {
        return messagingService;
    }

    public ScheduledExecutorService getScheduledExecutor() {
        return scheduledExecutor;
    }

    public List<String> getLogSensitiveDataKeys() {
        return logSensitiveDataKeys;
    }

    public int getMaxEventThreadCount() {
        return maxEventThreadCount;
    }
}
