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
import hu.dbx.kompot.exceptions.MessageErrorResultException;
import hu.dbx.kompot.exceptions.SerializationException;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static hu.dbx.kompot.impl.DataHandling.MethodResponseKeys.EXCEPTION_CLASS;
import static hu.dbx.kompot.impl.DataHandling.MethodResponseKeys.EXCEPTION_MESSAGE;

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

    public static String serializeMap(Map<String, Object> map) throws SerializationException {
        try {
            return new ObjectMapper().writeValueAsString(map);
        } catch (JsonProcessingException e) {
            throw new SerializationException(map, "Could not serialize event data");
        }
    }


    public static Map<String, Object> deserializeMap(String str) throws DeserializationException {
        try {
            return (Map<String, Object>) (new ObjectMapper().readValue(str, Map.class));
        } catch (IOException e) {
            throw new DeserializationException(str, "Could not deserialize Map");
        }
    }

    /**
     * Serializes event data of eventFrame.
     *
     * @throws SerializationException on failure
     */
    public static String serializeDataOnly(EventFrame frame) throws SerializationException {
        try {
            return new ObjectMapper().writeValueAsString(frame.getEventData());
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
        try {
            final Optional<EventDescriptor> marker = resolver.resolveMarker(eventName);
            if (!marker.isPresent())
                throw new IllegalArgumentException("Can not get marker for event name " + eventName);
            return (new ObjectMapper().readValue(content, marker.get().getRequestClass()));
        } catch (IOException e) {
            throw new DeserializationException(content, "Could not deserialize payload for " + eventName);
        }
    }

    public static Object deserializeResponse(String content, MethodDescriptor marker) throws DeserializationException {
        try {
            return (new ObjectMapper().readValue(content, marker.getResponseClass()));
        } catch (IOException e) {
            throw new DeserializationException(content, "Could not deserialize payload for " + marker.getMethodName());
        }
    }

    public static Object deserializeRequest(String methodName, String content, MethodDescriptorResolver resolver) throws DeserializationException {
        try {
            final Optional<MethodDescriptor> marker = resolver.resolveMarker(methodName);
            if (!marker.isPresent())
                throw new IllegalArgumentException("Can not get marker for method name " + methodName);
            return (new ObjectMapper().readValue(content, marker.get().getRequestClass()));
        } catch (IOException e) {
            throw new DeserializationException(content, "Could not deserialize payload for " + methodName);
        }
    }

    @SuppressWarnings("unchecked")
    public static Object deserializeBroadcast(BroadcastDescriptorResolver resolver, String broadcastCode, String content) throws DeserializationException {
        try {
            final Optional<BroadcastDescriptor> marker = resolver.resolveMarker(broadcastCode);
            if (!marker.isPresent())
                throw new IllegalArgumentException("Can not get broadcast type for name " + broadcastCode);
            return (new ObjectMapper().readValue(content, marker.get().getRequestClass()));
        } catch (IOException e) {
            throw new DeserializationException(content, "Could not deserialize payload for " + broadcastCode);
        }
    }

    public static String serializeObject(Object object) throws SerializationException {
        try {
            return new ObjectMapper().writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new SerializationException(object, "Could not serialize any data");
        }
    }

    public static MessageErrorResultException deserializeException(String methodDetailsKey, Jedis connection) {
        final String className = connection.hget(methodDetailsKey, EXCEPTION_CLASS.name());
        final String message = connection.hget(methodDetailsKey, EXCEPTION_MESSAGE.name());

        return new MessageErrorResultException(message, className);
    }
}