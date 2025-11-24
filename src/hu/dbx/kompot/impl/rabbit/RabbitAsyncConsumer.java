package hu.dbx.kompot.impl.rabbit;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.Listener;
import hu.dbx.kompot.events.Priority;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RabbitAsyncConsumer extends AbstractRabbitConsumer {

    private final String asyncQueueName;
    private final int maxEventThreadCount;
    private String asyncQueueConsumerTag;

    public RabbitAsyncConsumer(Connection connection, Listener listener, ConsumerIdentity consumerIdentity, Set<String> supportedBroadcastCodes, int maxEventThreadCount) {
        super(connection, listener, supportedBroadcastCodes);
        this.asyncQueueName = consumerIdentity.getEventGroup() + ".ASYNC";
        this.maxEventThreadCount = maxEventThreadCount;
    }

    protected Channel createChannel() throws IOException {
        final Channel channel = connection.createChannel();

        channel.basicQos(maxEventThreadCount, false); // prefetch limit per consumer
        channel.basicQos(maxEventThreadCount, true); // prefetch limit per channel

        final Map<String, Object> asyncQueueArgs = new HashMap<>();
        asyncQueueArgs.put("x-max-priority", Priority.getHighestPriority().score);
        channel.queueDeclare(asyncQueueName, true, false, false, asyncQueueArgs);
        asyncQueueConsumerTag = channel.basicConsume(asyncQueueName, false, getDeliverCallback(channel), consumerTag -> {
        });

        return channel;
    }

    @Override
    protected void stopConsuming() {
        try {
            channel.basicCancel(asyncQueueConsumerTag);
        } catch (Exception ignored) {
        }
    }

}
