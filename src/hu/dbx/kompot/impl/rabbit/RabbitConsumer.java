package hu.dbx.kompot.impl.rabbit;

import com.rabbitmq.client.*;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.Listener;
import hu.dbx.kompot.consumer.MessageResult;
import hu.dbx.kompot.events.Priority;
import hu.dbx.kompot.impl.LoggerUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RabbitConsumer implements Runnable {

    private static final Logger LOGGER = LoggerUtils.getLogger();

    private static final String BROADCAST_EXCHANGE_NAME = "broadcast";

    private final Connection connection;
    private final Listener listener;
    private final Thread daemonThread = new Thread(this);
    private Channel channel;
    private final String uniqueQueueName;
    private final String syncQueueName;
    private final String asyncQueueName;
    private final String asyncStandByQueueName;
    private final Set<String> supportedBroadcastCodes;

    public RabbitConsumer(Connection connection, Listener listener, ConsumerIdentity consumerIdentity, Set<String> supportedBroadcastCodes) {
        this.connection = connection;
        this.listener = listener;
        this.uniqueQueueName = consumerIdentity.getMessageGroup() + "." + consumerIdentity.getIdentifier();
        this.syncQueueName = consumerIdentity.getMessageGroup() + ".SYNC";
        this.asyncQueueName = consumerIdentity.getEventGroup() + ".ASYNC";
        this.asyncStandByQueueName = consumerIdentity.getEventGroup() + ".ASYNC.STANDBY";
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

    private Channel createChannel() throws IOException {
        final Channel channel = connection.createChannel();

        channel.basicQos(1, false); // prefetch limit per consumer
        channel.basicQos(1, true); // prefetch limit per channel

        final Map<String, Object> args = new HashMap<>();
        args.put("x-dead-letter-exchange", "");
        args.put("x-dead-letter-routing-key", asyncQueueName);
        args.put("x-message-ttl", 3000);
        channel.queueDeclare(asyncStandByQueueName, true, false, false, args);

        args.clear();
        args.put("x-max-priority", Priority.getHighestPriority().score);
        args.put("x-dead-letter-exchange", "");
        args.put("x-dead-letter-routing-key", asyncStandByQueueName);
        channel.queueDeclare(asyncQueueName, true, false, false, args);
        channel.basicConsume(asyncQueueName, false, getDeliverCallback(channel), consumerTag -> {
        });

        channel.queueDeclare(syncQueueName, false, false, true, null);
        channel.basicConsume(syncQueueName, false, getDeliverCallback(channel), consumerTag -> {
        });

        channel.queueDeclare(uniqueQueueName, false, true, true, null);
        channel.exchangeDeclare(BROADCAST_EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
        channel.queueBind(uniqueQueueName, BROADCAST_EXCHANGE_NAME, "");
        channel.basicConsume(uniqueQueueName, false, getDeliverCallback(channel), consumerTag -> {
        });

        return channel;
    }

    private DeliverCallback getDeliverCallback(final Channel channel) {
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
        final Object message = messageType.startsWith("b:") ? new String(delivery.getBody(), StandardCharsets.UTF_8) : delivery;
        if (!messageType.startsWith("b:") || supportedBroadcastCodes.contains(messageType.substring(2))) {
            return listener.onMessage(messageType, message);
        }
        return MessageResult.SKIPPED;
    }

    private void handleResult(final Channel channel, final Delivery delivery, final MessageResult result) {
        try {
            if (result == MessageResult.REJECTED) {
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);
            } else {
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        } catch (Throwable throwable) {
            LOGGER.error(throwable.getMessage());
        }
    }

}
