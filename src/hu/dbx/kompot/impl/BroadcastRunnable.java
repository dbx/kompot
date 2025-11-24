package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.broadcast.handler.SelfDescribingBroadcastProcessor;
import hu.dbx.kompot.core.MessagingService;
import hu.dbx.kompot.core.SerializeHelper;
import hu.dbx.kompot.exceptions.DeserializationException;
import hu.dbx.kompot.impl.consumer.ConsumerHandlers;
import org.slf4j.Logger;

import java.util.Optional;

public final class BroadcastRunnable implements Runnable {

    private static final Logger LOGGER = LoggerUtils.getLogger();

    private final String broadcastCode;
    private final Object message;
    private final ConsumerHandlers consumerHandlers;
    private final MessagingService messagingService;

    public BroadcastRunnable(String broadcastCode, Object message, ConsumerHandlers consumerHandlers, MessagingService messagingService) {
        this.broadcastCode = broadcastCode;
        this.message = message;
        this.consumerHandlers = consumerHandlers;
        this.messagingService = messagingService;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void run() {
        final Optional<BroadcastDescriptor> descriptor = consumerHandlers.getBroadcastDescriptorResolver().resolveMarker(broadcastCode);
        if (!descriptor.isPresent()) {
            LOGGER.error("Did not find descriptor for broadcast code {}", broadcastCode);
            return;
        }
        final String data = (String) messagingService.getBroadcastData(message);
        try {
            LOGGER.debug("Deserializing broadcast data...");
            final Object dataObj = SerializeHelper.deserializeBroadcast(consumerHandlers.getBroadcastDescriptorResolver(), broadcastCode, data);
            LOGGER.debug("Deserialized broadcast data of type {}", dataObj.getClass());

            Optional<SelfDescribingBroadcastProcessor> factory = consumerHandlers.getBroadcastProcessorFactory().create(descriptor.get());
            if (!factory.isPresent()) {
                // ez elvileg nem lehetseges, mert csak azokra iratkozunk fel, amikre tudunk is figyelni.
                LOGGER.error("Illegalis allapot, nincsen broadcast a keresett '{}' tipusra!", broadcastCode);
            } else {
                LOGGER.debug("Handling broadcast {}", broadcastCode);
                factory.get().handle(dataObj);
                LOGGER.debug("Successfully handled broadcast {}", broadcastCode);
            }
        } catch (DeserializationException e) {
            LOGGER.error("Could not deserialize broadcast payload for code {} and data {}", broadcastCode, data);
        } catch (Throwable t) {
            LOGGER.error("Error handling broadcast code=" + broadcastCode + " data=" + data, t);
        } finally {
            messagingService.afterMessageProcessed(message);
        }
    }

}
