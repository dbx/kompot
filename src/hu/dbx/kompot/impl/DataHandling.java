package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.EventDescriptorResolver;
import hu.dbx.kompot.consumer.async.EventFrame;
import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.consumer.sync.MethodDescriptorResolver;
import hu.dbx.kompot.consumer.sync.MethodRequestFrame;
import hu.dbx.kompot.core.SerializeHelper;
import hu.dbx.kompot.exceptions.DeserializationException;
import hu.dbx.kompot.moby.MetaDataHolder;
import hu.dbx.kompot.producer.ProducerIdentity;
import org.slf4j.Logger;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

public final class DataHandling {

    private static final Logger LOGGER = LoggerUtils.getLogger();

    private static final int MAX_ENTITY_SIZE = 8 * 1024;

    public enum EventKeys {

        /**
         * Event code (name)
         */
        CODE,

        /**
         * Serialized event data
         *
         * @deprecated All data is sent compressed now
         */
        @Deprecated
        DATA,

        /**
         * Compressed serialized event data.
         */
        DATA_ZIP,

        /**
         * Comma separated list of event names
         */
        GROUPS,

        /**
         * Integer value representing the number unprocessed groups
         */
        UNPROCESSED_GROUPS,

        /**
         * UUID of producer who initiated this event
         */
        SENDER,

        /**
         * Time when event was registered
         */
        FIRST_SENT,

        STATUS,

        /**
         * Priority if evt, either HIGH or LOW.
         */
        PRIORITY,

        ERROR_MSG
    }

    public enum Statuses {
        CREATED, ERROR, PROCESSED, PROCESSING
    }

    public enum MethodResponseKeys {
        RESPONSE, STATUS, EXCEPTION_CLASS, EXCEPTION_MESSAGE
    }

    public enum EventPriority {HI, LO}

    @SuppressWarnings("unchecked")
    public static EventFrame<?> readEventFrame(String eventName,
                                            String eventData,
                                            byte[] eventDataZip,
                                            MetaDataHolder metaDataHolder,
                                            EventDescriptorResolver eventResolver,
                                            UUID eventUuid,
                                            String eventSender,
                                            List<String> logSensitiveDataKeys) throws DeserializationException {
        if (eventName == null) {
            throw new IllegalArgumentException("Empty event name for eventUuid=" + eventUuid);
        }

        final Object eventDataObj;

        if (eventData != null) {
            eventDataObj = SerializeHelper.deserializeContentOnly(eventName, eventData, eventResolver);
        } else {
            eventDataObj = decompressEventData(eventResolver, eventName, eventDataZip, logSensitiveDataKeys);
        }

        final Optional<EventDescriptor> eventMarker = eventResolver.resolveMarker(eventName);

        if (!eventMarker.isPresent()) {
            String template = "Could not resolve event marker for event code=%s uuid=%s";
            String msg = String.format(template, eventName, eventUuid.toString());
            LOGGER.error(msg);
            throw new IllegalStateException(msg);
        } else {
            EventFrame response = new EventFrame();
            response.setEventMarker(eventMarker.get());
            response.setEventData(eventDataObj);
            response.setIdentifier(eventUuid);
            response.setSourceIdentifier(eventSender);
            response.setMetaData(metaDataHolder);
            return response;
        }
    }

    private static Object decompressEventData(EventDescriptorResolver eventResolver, String eventName, byte[] eventDataZip, List<String> logSensitiveDataKeys) throws DeserializationException {
        Object eventDataObj;
        try (ByteArrayInputStream input = new ByteArrayInputStream(eventDataZip); GZIPInputStream iz = new GZIPInputStream(input)) {
            eventDataObj = SerializeHelper.deserializeContentOnly(eventName, logEntityStream(iz, logSensitiveDataKeys), eventResolver);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return eventDataObj;
    }

    @SuppressWarnings("unchecked")
    public static Optional<MethodRequestFrame<?>> readMethodFrame(String methodName,
                                                               String methodData,
                                                               byte[] methodDataZip,
                                                               MetaDataHolder methodMeta,
                                                               MethodDescriptorResolver resolver,
                                                               UUID methodUuid,
                                                               String sender,
                                                               List<String> logSensitiveDataKeys) throws DeserializationException {
        final Optional<MethodDescriptor> descriptor = resolver.resolveMarker(methodName);
        if (!descriptor.isPresent()) {
            LOGGER.warn("Could not resolve method descriptor for {}", methodName);
            return Optional.empty();
        }

        final Object requestData;

        if (methodData != null) {
            requestData = SerializeHelper.deserializeRequest(methodName, methodData, resolver);
        } else {
            requestData = decompressMethodData(resolver, methodName, methodDataZip, logSensitiveDataKeys);
        }


        final MethodRequestFrame<?> frame = MethodRequestFrame.build(
                methodUuid,
                new ProducerIdentity.CustomIdentity(sender),
                descriptor.get(),
                requestData,
                methodMeta);

        return Optional.of(frame);
    }

    private static Object decompressMethodData(MethodDescriptorResolver resolver, String methodName, byte[] methodDataZip, List<String> logSensitiveDataKeys) throws DeserializationException {
        Object requestData;
        try (ByteArrayInputStream input = new ByteArrayInputStream(methodDataZip); GZIPInputStream iz = new GZIPInputStream(input)) {
            requestData = SerializeHelper.deserializeRequest(methodName, logEntityStream(iz, logSensitiveDataKeys), resolver);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return requestData;
    }

    private static InputStream logEntityStream(InputStream stream, List<String> logSensitiveDataKeys) throws IOException {
        final StringBuilder builder = new StringBuilder();

        builder.append("DataEntity:\n");

        if (!stream.markSupported()) {
            stream = new BufferedInputStream(stream);
        }
        stream.mark(MAX_ENTITY_SIZE + 1);
        final byte[] entity = new byte[MAX_ENTITY_SIZE + 1];
        final int entitySize = stream.read(entity);
        if (entitySize > 0) {
            builder.append(new String(entity, 0, Math.min(entitySize, MAX_ENTITY_SIZE)));
            if (entitySize > MAX_ENTITY_SIZE) {
                builder.append("...more...");
            }
            builder.append('\n');
        }
        stream.reset();

        final String s = filterSensitiveDataOutFromMessage(builder.toString(), logSensitiveDataKeys);
        LOGGER.info(s);

        return stream;
    }

    public static String filterSensitiveDataOutFromMessage(String message, List<String> logSensitiveDataKeys) {
        for (String key : logSensitiveDataKeys) {
            message = filterByKeyword(key, message);
        }

        return message;
    }

    /**
     * Filters out the value of a key in a json(or map)-like string.
     * Examples:
     * 1) "password": "asd123" -> "password": <FILTERED>
     * 2) password=asd123 -> password=<FILTERED>
     */
    private static String filterByKeyword(final String keyword, final String requestString) {
        return requestString.replaceAll("(?is)(\"?" + keyword + "\"?\\s*[:=]\\s*)(\"?)([^\"?,}\\s]*)(\"?)", "$1<FILTERED>");
    }


}
