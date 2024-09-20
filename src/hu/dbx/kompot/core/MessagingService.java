package hu.dbx.kompot.core;

import hu.dbx.kompot.consumer.Listener;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.EventFrame;
import hu.dbx.kompot.consumer.async.EventReceivingCallback;
import hu.dbx.kompot.consumer.async.EventStatusCallback;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.sync.MethodRequestFrame;
import hu.dbx.kompot.exceptions.MessageErrorResultException;
import hu.dbx.kompot.impl.ConsumerImpl;
import hu.dbx.kompot.impl.DataHandling;
import hu.dbx.kompot.impl.consumer.ConsumerConfig;
import hu.dbx.kompot.impl.consumer.ConsumerHandlers;
import hu.dbx.kompot.impl.producer.ProducerConfig;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public interface MessagingService {

    void start(Listener listener, Set<String> supportedBroadcastCodes) throws InterruptedException;

    void stop() throws InterruptedException;

    void afterStarted(ConsumerImpl consumer, AtomicInteger processingEvents, ConsumerHandlers consumerHandlers, List<EventReceivingCallback> eventReceivingCallbacks);

    void afterStopped(ConsumerConfig consumerConfig);

    void sendEvent(EventDescriptor<?> marker, EventFrame<?> eventFrame, ProducerConfig producerConfig);

    void sendMessage(MethodRequestFrame<?> requestFrame, ProducerConfig producerConfig);

    void broadcast(BroadcastDescriptor<?> descriptor, String serializedData);

    EventStatusCallback getEventStatusCallback(Object message, List<EventReceivingCallback> eventReceivingCallbacks);

    void afterEvent(ConsumerImpl consumerImpl, AtomicInteger processingEvents, ConsumerHandlers consumerHandlers, List<EventReceivingCallback> eventReceivingCallbacks);

    Optional<EventFrame<?>> getEventFrame(Object message, ConsumerConfig consumerConfig, ConsumerHandlers consumerHandlers) throws Exception;

    Optional<DataHandling.Statuses> getMethodStatus(Object message, ProducerConfig producerConfig, MethodRequestFrame<?> requestFrame);

    MessageErrorResultException getMethodError(Object message, ProducerConfig producerConfig, MethodRequestFrame<?> requestFrame);

    byte[] getMethodResponse(Object message, ProducerConfig producerConfig, MethodRequestFrame<?> requestFrame);

    Optional<MethodRequestFrame<?>> getMethodRequestFrame(Object message, ConsumerConfig consumerConfig, ConsumerHandlers consumerHandlers) throws Exception;

    void sendMethodRespond(Object response, Object message, MethodRequestFrame<?> mrf, Throwable throwable);

    UUID getMessageUuid(Object message);

}
