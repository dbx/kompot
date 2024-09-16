package hu.dbx.kompot.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.EventDescriptorResolver;
import hu.dbx.kompot.consumer.async.EventFrame;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptor;
import hu.dbx.kompot.consumer.broadcast.handler.BroadcastDescriptorResolver;
import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.consumer.sync.MethodDescriptorResolver;
import hu.dbx.kompot.exceptions.DeserializationException;
import hu.dbx.kompot.exceptions.SerializationException;
import hu.dbx.kompot.status.StatusReport;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringBufferInputStream;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPOutputStream;

/**
 * Serialization strategy for EventFrame entities.
 * <p>
 * We use JSON serialization by default.
 * <p>
 * A SEPARATOR String is used to separate parts of the payload. The parts are the following:
 * <p>
 * 1. eventName - from the event marker instance. Also used for deserialization.
 * 2. sender identifier (uuid string)
 * 3. data payload.
 */
public final class SerializeHelper {

    /**
     * Token to separate parts of the serialized event frame instance.
     * Changing it will break already serialized in persisted store.
     */
    private final static String SEPARATOR = ",";

    private final static AtomicReference<ObjectMapper> objectMapper = new AtomicReference<>(new ObjectMapper());

    public static ObjectMapper getObjectMapper() {
        return objectMapper.get();
    }

    public static void setObjectMapper(ObjectMapper newObjectMapper) {
        objectMapper.set(newObjectMapper);
    }

    public static String serializeMap(Map<String, Object> map) throws SerializationException {
        try {
            return getObjectMapper().writeValueAsString(map);
        } catch (JsonProcessingException e) {
            throw new SerializationException(map, "Could not serialize event data");
        }
    }

    public static Map<String, Object> deserializeMap(String str) throws DeserializationException {
        try {
            //noinspection unchecked
            return (Map<String, Object>) (getObjectMapper().readValue(str, Map.class));
        } catch (Throwable e) {
            throw new DeserializationException(str, "Could not deserialize Map", e);
        }
    }

    public static StatusReport deserializeStatus(String str) throws DeserializationException {
        try {
            //noinspection unchecked
            return (getObjectMapper().readValue(str, StatusReport.class));
        } catch (IOException e) {
            throw new DeserializationException(str, "Could not deserialize status report", e);
        }
    }

    /**
     * Serializes event data of eventFrame.
     *
     * @throws SerializationException on failure
     */
    public static String serializeDataOnly(EventFrame frame) throws SerializationException {
        try {
            return getObjectMapper().writeValueAsString(frame.getEventData());
        } catch (JsonProcessingException e) {
            throw new SerializationException(frame, "Could not serialize event data");
        }
    }

    /**
     * Deserializes content string and returns data payload object.
     * Uses event name and marker resolver to get target class of data.
     *
     * @throws DeserializationException when failed to read binary to object
     * @throws IllegalArgumentException when no event marker is resolved for name
     */
    @SuppressWarnings("unchecked")
    public static Object deserializeContentOnly(String eventName, String content, EventDescriptorResolver resolver) throws DeserializationException {
        return deserializeContentOnly(eventName, new StringBufferInputStream(content), resolver);
    }

    public static Object deserializeContentOnly(String eventName, InputStream input, EventDescriptorResolver resolver) throws DeserializationException {
        final Optional<EventDescriptor> marker = resolver.resolveMarker(eventName);
        if (!marker.isPresent()) {
            throw new IllegalArgumentException("Can not get marker for event name " + eventName);
        }

        final Class targetClass = marker.get().getRequestClass();

        try {
            return (getObjectMapper().readValue(input, targetClass));
        } catch (Exception e) {
            String message = "Could not deserialize payload for event " + eventName + ", class: " + targetClass;
            throw new DeserializationException("-", message, e);
        }
    }

    public static Object deserializeResponse(String content, MethodDescriptor marker) throws DeserializationException {
        return deserializeResponse(new StringBufferInputStream(content), marker);
    }


    public static Object deserializeResponse(InputStream content, MethodDescriptor marker) throws DeserializationException {
        final Class targetClass = marker.getResponseClass();

        try {
            //noinspection UnnecessaryParentheses,unchecked
            return (getObjectMapper().readValue(content, targetClass));
        } catch (Exception e) {
            String message = "Could not deserialize payload for method " + marker.getMethodName() + ", class: " + targetClass;
            throw new DeserializationException("-", message, e);
        }
    }

    public static Object deserializeRequest(String methodName, String content, MethodDescriptorResolver resolver) throws DeserializationException {
        return deserializeRequest(methodName, new StringBufferInputStream(content), resolver);
    }

    public static Object deserializeRequest(String methodName, InputStream content, MethodDescriptorResolver resolver) throws DeserializationException {
        final Optional<MethodDescriptor> marker = resolver.resolveMarker(methodName);

        if (!marker.isPresent()) {
            throw new IllegalArgumentException("Can not get marker for method name " + methodName);
        }

        final Class targetClass = marker.get().getRequestClass();

        try {
            //noinspection unchecked
            return (getObjectMapper().readValue(content, targetClass));
        } catch (Exception e) {
            String message = "Could not deserialize payload for method " + methodName + " class: " + targetClass;
            throw new DeserializationException("-", message, e);
        }
    }

    @SuppressWarnings("unchecked")
    public static Object deserializeBroadcast(BroadcastDescriptorResolver resolver, String broadcastCode, String content) throws DeserializationException {
        final Optional<BroadcastDescriptor> marker = resolver.resolveMarker(broadcastCode);
        if (!marker.isPresent()) {
            throw new IllegalArgumentException("Can not get broadcast type for name " + broadcastCode);
        }
        Class targetClass = marker.get().getRequestClass();

        try {
            return (getObjectMapper().readValue(content, targetClass));
        } catch (Exception e) {
            String message = "Could not deserialize payload for broadcast " + broadcastCode + ", class: " + targetClass;
            throw new DeserializationException(content, message, e);
        }
    }

    public static String serializeObject(Object object) throws SerializationException {
        try {
            return getObjectMapper().writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new SerializationException(object, "Could not serialize any data", e);
        }
    }

    public static byte[] serializeObjectToBytes(Object object) {
        try {
            return getObjectMapper().writeValueAsBytes(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] compressData(Object eventData) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(); GZIPOutputStream iz = new GZIPOutputStream(out)) {
            SerializeHelper.getObjectMapper().writeValue(iz, eventData);
            iz.close();
            out.close();
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}