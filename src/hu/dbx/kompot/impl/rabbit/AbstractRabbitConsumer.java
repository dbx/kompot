package hu.dbx.kompot.impl.rabbit;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import hu.dbx.kompot.consumer.Listener;
import hu.dbx.kompot.consumer.MessageResult;
import hu.dbx.kompot.impl.LoggerUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Set;

abstract class AbstractRabbitConsumer implements Runnable {

    private static final Logger LOGGER = LoggerUtils.getLogger();

    protected final Connection connection;
    private final Listener listener;
    private final Thread daemonThread = new Thread(this);
    protected Channel channel;
    private final Set<String> supportedBroadcastCodes;

    public AbstractRabbitConsumer(Connection connection, Listener listener, Set<String> supportedBroadcastCodes) {
        this.connection = connection;
        this.listener = listener;
        this.supportedBroadcastCodes = supportedBroadcastCodes;
    }

    @Override
    public void run() {
        try {
            channel = createChannel();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public void start() {
        daemonThread.start();
        listener.afterStarted();
    }

    public void stop() {
        try {
            channel.close();
        } catch (Exception ignored) {
        }
        listener.afterStopped();
    }

    protected abstract Channel createChannel() throws IOException;

    protected abstract void stopConsuming();

    protected DeliverCallback getDeliverCallback(final Channel channel) {
        return (consumerTag, delivery) -> {
            MessageResult result = MessageResult.PROCESSING;
            try {
                result = processMessage(delivery);
            } catch (Throwable throwable) {
                result = MessageResult.ERROR;
                LOGGER.debug(throwable.getMessage());
            } finally {
                handleResult(channel, delivery, result);
            }
        };
    }

    private MessageResult processMessage(final Delivery delivery) {
        final String messageType = delivery.getProperties().getType();
        if (!messageType.startsWith("b:") || supportedBroadcastCodes.contains(messageType.substring(2))) {
            return listener.onMessage(messageType, delivery);
        }
        return MessageResult.SKIPPED;
    }

    private void handleResult(final Channel channel, final Delivery delivery, final MessageResult result) {
        try {
            if (result == MessageResult.REJECTED) {
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
            }
            if (result == MessageResult.ERROR || result == MessageResult.SKIPPED) {
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        } catch (Throwable throwable) {
            LOGGER.error(throwable.getMessage());
        }
    }

    public void acknowledgeDelivery(final Delivery delivery) {
        try {
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        } catch (Throwable throwable) {
            LOGGER.error(throwable.getMessage());
        }
    }

    public boolean isOpen() {
        return channel != null && channel.isOpen();
    }

}
