package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.consumer.sync.MethodReceivingCallback;
import hu.dbx.kompot.consumer.sync.MethodRequestFrame;
import hu.dbx.kompot.core.MessagingService;
import hu.dbx.kompot.impl.consumer.ConsumerConfig;
import hu.dbx.kompot.impl.consumer.ConsumerHandlers;
import org.slf4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static hu.dbx.kompot.impl.LoggerUtils.debugMethodFrame;

/**
 * Represents a job for handling a method request.
 */
final class MethodRunnable implements Runnable {

    private static final Logger LOGGER = LoggerUtils.getLogger();

    private final ConsumerImpl consumer;
    private final Object message;
    private final ConsumerConfig consumerConfig;
    private final List<MethodReceivingCallback> methodEventListeners;
    private final ConsumerHandlers consumerHandlers;
    private final MessagingService messagingService;
    private final UUID messageUuid;

    MethodRunnable(ConsumerImpl consumer, List<MethodReceivingCallback> methodEventListeners, ConsumerHandlers consumerHandlers, Object message) {
        this.consumer = consumer;
        this.consumerConfig = consumer.getConsumerConfig();
        this.methodEventListeners = methodEventListeners;
        this.consumerHandlers = consumerHandlers;
        this.message = message;
        this.messagingService = consumerConfig.getMessagingService();
        this.messageUuid = messagingService.getMessageUuid(message);

    }

    @Override
    public void run() {
        //noinspection rawtypes
        MethodRequestFrame mrf = null;
        Object response = null;
        Throwable throwable = null;

        // itt egy try-catch van, mert csak akkor irhatom vissza, hogy nem sikerult, ha mar enyem az ownership.
        try {
            final Optional<MethodRequestFrame<?>> frameOp = messagingService.getMethodRequestFrame(message, consumer.getConsumerConfig(), consumerHandlers);

            if (frameOp.isPresent()) {
                mrf = frameOp.get();
                response = process(mrf);
            } else {
                LOGGER.debug("Could not read method request frame from message {}", messageUuid);
                // lejart a metodus mielott ki tudtuk volna olvasni?
            }
        } catch (Throwable t) {
            LOGGER.error("Exception happened when sending method");
            debugMethodFrame(LOGGER, mrf);
            LOGGER.error("Method exception: ", t);
            throwable = t;
            if (mrf != null) {
                callFailureListeners(mrf, t);
            }
        } finally {
            messagingService.sendMethodRespond(response, message, mrf, throwable);
        }
    }

    private Object process(@SuppressWarnings("rawtypes") MethodRequestFrame mrf) {
        //noinspection rawtypes
        final MethodDescriptor methodMarker = mrf.getMethodMarker();

        // esemenykezelok futtatasa
        methodEventListeners.forEach(x -> {
            try {
                x.onRequestReceived(mrf);
            } catch (Throwable t) {
                LOGGER.error("Error when running method sending event listener {} for message {}", x, messageUuid);
            }
        });

        LOGGER.info("Received method calling from {} to {}/{} with meta {}", mrf.getMetaData().getSourceName(),
                methodMarker.getMethodGroupName(), methodMarker.getMethodName(), mrf.getMetaData());

        LOGGER.trace("Calling method processor for {}/{}", methodMarker.getMethodGroupName(), methodMarker.getMethodName());
        //noinspection unchecked
        final Object response = consumer.getMethodProcessorAdapter().call(methodMarker, mrf.getMethodData(), mrf.getMetaData());
        LOGGER.trace("Called method processor for {}/{}", methodMarker.getMethodGroupName(), methodMarker.getMethodName());

        methodEventListeners.forEach(x -> {
            try {
                x.onRequestProcessedSuccessfully(mrf, response);
            } catch (Throwable t) {
                debugMethodFrame(LOGGER, mrf);
                LOGGER.error("Error when running method sending event listener.", t);
            }
        });

        LOGGER.debug("Written response to method {}/{} to message {}", methodMarker.getMethodGroupName(), methodMarker.getMethodName(), messageUuid);

        return response;
    }

    /**
     * On case of failures we run callbacks and write failure code.
     */
    private void callFailureListeners(@SuppressWarnings("rawtypes") MethodRequestFrame mrf, Throwable t) {
        methodEventListeners.forEach(x -> {
            try {
                x.onRequestProcessingFailure(mrf, t);
            } catch (Throwable e) {
                LOGGER.error("Error when running method failure event listener for message {} on {}", x, messageUuid, e);
            }
        });
    }
}
