package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.broadcast.handler.SelfDescribingBroadcastProcessor;
import hu.dbx.kompot.core.SerializeHelper;
import hu.dbx.kompot.exceptions.DeserializationException;
import hu.dbx.kompot.impl.consumer.ConsumerHandlers;
import org.slf4j.Logger;

import java.util.Optional;

public final class BroadcastRunnable implements Runnable {

    private static final Logger LOGGER = LoggerUtils.getLogger();

    private final String broadcastCode;
    private final String data;
    private final ConsumerHandlers consumerHandlers;

    public BroadcastRunnable(String broadcastCode, String data, ConsumerHandlers consumerHandlers) {
        this.broadcastCode = broadcastCode;
        this.data = data;
        this.consumerHandlers = consumerHandlers;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void run() {
        final Optional<BroadcastDescriptor> descriptor = consumerHandlers.getBroadcastDescriptorResolver().resolveMarker(broadcastCode);
        if (!descriptor.isPresent()) {
            LOGGER.error("Did not find descriptor for broadcast code {}", broadcastCode);
            return;
        }
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
        }
    }

}
