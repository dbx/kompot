package hu.dbx.kompot.impl.rabbit;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.Listener;

import java.io.IOException;
import java.util.Set;

public class RabbitConsumer extends AbstractRabbitConsumer {

    private static final String BROADCAST_EXCHANGE_NAME = "broadcast";

    private final String uniqueQueueName;
    private final String syncQueueName;
    private String syncQueueConsumerTag;

    public RabbitConsumer(Connection connection, Listener listener, ConsumerIdentity consumerIdentity, Set<String> supportedBroadcastCodes) {
        super(connection, listener, supportedBroadcastCodes);
        this.uniqueQueueName = consumerIdentity.getMessageGroup() + "." + consumerIdentity.getIdentifier();
        this.syncQueueName = consumerIdentity.getMessageGroup() + ".SYNC";
    }

    protected Channel createChannel() throws IOException {
        final Channel channel = connection.createChannel();

        channel.basicQos(12, false); // prefetch limit per consumer
        channel.basicQos(12, true); // prefetch limit per channel

        channel.queueDeclare(syncQueueName, true, false, true, null);
        syncQueueConsumerTag = channel.basicConsume(syncQueueName, false, getDeliverCallback(channel), consumerTag -> {
        });

        channel.queueDeclare(uniqueQueueName, false, true, true, null);
        channel.exchangeDeclare(BROADCAST_EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
        channel.queueBind(uniqueQueueName, BROADCAST_EXCHANGE_NAME, "");
        channel.basicConsume(uniqueQueueName, false, getDeliverCallback(channel), consumerTag -> {
        });

        return channel;
    }

    @Override
    protected void stopConsuming() {
        try {
            channel.basicCancel(syncQueueConsumerTag);
        } catch (Exception ignored) {
        }
    }

}
