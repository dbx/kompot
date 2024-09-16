package hu.dbx.kompot.impl.rabbit;

import com.rabbitmq.client.*;
import hu.dbx.kompot.consumer.ConsumerIdentity;
import hu.dbx.kompot.consumer.Listener;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.EventFrame;
import hu.dbx.kompot.consumer.async.EventReceivingCallback;
import hu.dbx.kompot.consumer.async.EventStatusCallback;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.sync.MethodRequestFrame;
import hu.dbx.kompot.core.MessagingService;
import hu.dbx.kompot.core.SerializeHelper;
import hu.dbx.kompot.exceptions.MessageErrorResultException;
import hu.dbx.kompot.impl.ConsumerImpl;
import hu.dbx.kompot.impl.DataHandling;
import hu.dbx.kompot.impl.DefaultEventStatusCallback;
import hu.dbx.kompot.impl.LoggerUtils;
import hu.dbx.kompot.impl.consumer.ConsumerConfig;
import hu.dbx.kompot.impl.consumer.ConsumerHandlers;
import hu.dbx.kompot.impl.producer.ProducerConfig;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static hu.dbx.kompot.impl.DataHandling.Statuses.ERROR;
import static hu.dbx.kompot.impl.DataHandling.Statuses.PROCESSED;

public class RabbitMessagingService implements MessagingService {

    private static final String BROADCAST_EXCHANGE_NAME = "broadcast";

    private static final Logger LOGGER = LoggerUtils.getLogger();

    private final Connection connection;
    private final ConsumerIdentity consumerIdentity;
    private RabbitConsumer rabbitConsumer;

    public RabbitMessagingService(Connection connection, ConsumerIdentity consumerIdentity) {
        this.connection = connection;
        this.consumerIdentity = consumerIdentity;
    }

    @Override
    public void start(Listener listener, Set<String> supportedBroadcastCodes) throws InterruptedException {
        this.rabbitConsumer = new RabbitConsumer(connection, listener, consumerIdentity, supportedBroadcastCodes);
        rabbitConsumer.start();
    }

    @Override
    public void stop() {
        rabbitConsumer.stop();
    }

    @Override
    public void afterStarted(ConsumerImpl consumer, AtomicInteger processingEvents, ConsumerHandlers consumerHandlers, List<EventReceivingCallback> eventReceivingCallbacks) {
    }

    @Override
    public void afterStopped(ConsumerConfig consumerConfig) {
    }

    @Override
    public void sendEvent(EventDescriptor<?> marker, EventFrame<?> eventFrame, ProducerConfig producerConfig) {
        final byte[] eventDataZip = SerializeHelper.compressData(eventFrame.getEventData());
        final byte[] data = SerializeHelper.serializeObjectToBytes(new DataWrapper(eventDataZip, eventFrame.getMetaData()));

        final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .messageId(eventFrame.getIdentifier().toString())
                .correlationId(producerConfig.getProducerIdentity().getIdentifier())
                .type("e:" + eventFrame.getEventMarker().getEventName())
                .priority(marker.getPriority().score)
                .build();

        for (String group : eventFrame.getEventGroups()) {
            try (Channel channel = connection.createChannel()) { // TODO: channel-t csak egyszer nyitni?
                channel.basicPublish("", group + ".ASYNC", properties, data);
            } catch (IOException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void sendMessage(MethodRequestFrame<?> requestFrame, ProducerConfig producerConfig) {
        final byte[] methodDataZip = SerializeHelper.compressData(requestFrame.getMethodData());
        final byte[] data = SerializeHelper.serializeObjectToBytes(new DataWrapper(methodDataZip, requestFrame.getMetaData()));
        // TODO: final int expiration = ((int) Math.ceil(((double) requestFrame.getMethodMarker().getTimeout()) / 1000));
        try (Channel channel = connection.createChannel()) {

            final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .messageId(requestFrame.getIdentifier().toString())
                    .correlationId(requestFrame.getSourceIdentifier())
                    .replyTo(consumerIdentity.getMessageGroup() + "." + requestFrame.getSourceIdentifier())
                    .type("m:" + requestFrame.getMethodMarker().getMethodName())
                    .build();

            channel.basicPublish("", requestFrame.getMethodMarker().getMethodGroupName() + ".SYNC", properties, data);
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void broadcast(BroadcastDescriptor<?> descriptor, String serializedData) {
        try (Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(BROADCAST_EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
            final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .type("b:" + descriptor.getBroadcastCode())
                    .build();
            channel.basicPublish(BROADCAST_EXCHANGE_NAME, "", properties, serializedData.getBytes(StandardCharsets.UTF_8));
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public EventStatusCallback getEventStatusCallback(Object message, List<EventReceivingCallback> eventReceivingCallbacks) {
        return new DefaultEventStatusCallback(getMessageUuid(message), eventReceivingCallbacks);
    }

    @Override
    public void afterEvent(ConsumerImpl consumer, AtomicInteger processingEvents, ConsumerHandlers consumerHandlers, List<EventReceivingCallback> eventReceivingCallbacks) {
        processingEvents.decrementAndGet();
    }

    @Override
    public Optional<EventFrame<?>> getEventFrame(Object message, ConsumerConfig consumerConfig, ConsumerHandlers consumerHandlers) throws Exception {
        final Delivery delivery = ((Delivery) message);
        final BasicProperties properties = delivery.getProperties();
        final String messageType = properties.getType();
        final String eventName = messageType.substring(2);
        final UUID eventUuid = UUID.fromString(properties.getMessageId());
        final String eventSender = properties.getCorrelationId();
        final DataWrapper dataWrapper = RabbitHelper.deserializeDataWrapper(delivery.getBody());

        return Optional.of(DataHandling.readEventFrame(eventName,
                null,
                dataWrapper.getData(),
                dataWrapper.getMetaDataHolder(),
                consumerHandlers.getEventResolver(),
                eventUuid,
                eventSender,
                consumerConfig.getLogSensitiveDataKeys()));
    }

    @Override
    public Optional<DataHandling.Statuses> getMethodStatus(Object message, ProducerConfig producerConfig, MethodRequestFrame<?> requestFrame) {
        return Optional.of(getMethodDataWrapper(message).getStatus());
    }

    @Override
    public MessageErrorResultException getMethodError(Object message, ProducerConfig producerConfig, MethodRequestFrame<?> requestFrame) {
        final MethodDataWrapper wrapper = getMethodDataWrapper(message);
        return new MessageErrorResultException(wrapper.getExceptionMessage(), wrapper.getExceptionClass());
    }

    @Override
    public byte[] getMethodResponse(Object message, ProducerConfig producerConfig, MethodRequestFrame<?> requestFrame) {
        return getMethodDataWrapper(message).getData();
    }

    private MethodDataWrapper getMethodDataWrapper(final Object message) {
        final Delivery delivery = ((Delivery) message);
        return RabbitHelper.deserializeMethodDataWrapper(delivery.getBody());
    }

    @Override
    public Optional<MethodRequestFrame<?>> getMethodRequestFrame(Object message, ConsumerConfig consumerConfig, ConsumerHandlers consumerHandlers) throws Exception {
        final Delivery delivery = ((Delivery) message);
        final BasicProperties properties = delivery.getProperties();
        final String messageType = properties.getType();
        final String methodName = messageType.substring(2);
        final UUID methodUuid = UUID.fromString(properties.getMessageId());
        final String methodSender = properties.getCorrelationId();
        final DataWrapper dataWrapper = RabbitHelper.deserializeDataWrapper(delivery.getBody());

        return DataHandling.readMethodFrame(methodName,
                null,
                dataWrapper.getData(),
                dataWrapper.getMetaDataHolder(),
                consumerHandlers.getMethodDescriptorResolver(),
                methodUuid,
                methodSender,
                consumerConfig.getLogSensitiveDataKeys());
    }

    @Override
    public void sendMethodRespond(Object response, Object message, MethodRequestFrame<?> mrf, Throwable throwable) {
        final String replyTo = getReplyToFromMessage(message);
        LOGGER.debug("Notifying response on {} with {}", replyTo, mrf.getIdentifier());

        // TODO: expiration???
        final byte[] responseDataZip = SerializeHelper.compressData(response);
        final MethodDataWrapper wrapper = new MethodDataWrapper(responseDataZip, mrf.getMetaData());
        if (throwable != null) {
            wrapper.setExceptionMessage(throwable.getMessage());
            wrapper.setExceptionClass(throwable.getClass().getName());
            wrapper.setStatus(ERROR);
        } else {
            wrapper.setStatus(PROCESSED);
        }
        final byte[] data = SerializeHelper.serializeObjectToBytes(wrapper);

        try (Channel channel = connection.createChannel()) {
            final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .messageId(mrf.getIdentifier().toString())
                    .correlationId(consumerIdentity.getIdentifier())
                    .type("id:" + replyTo)
                    .build();

            channel.basicPublish("", getReplyToFromMessage(message), properties, data);
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private String getReplyToFromMessage(Object message) {
        return ((Delivery) message).getProperties().getReplyTo();
    }

    @Override
    public UUID getMessageUuid(Object message) {
        try {
            return UUID.fromString(((Delivery) message).getProperties().getMessageId());
        } catch (Exception e) {
            LOGGER.error("Could not get message uuid from message ({}) {}", message, e);
            return null;
        }
    }

}
