package hu.dbx.kompot.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Optional;

// TODO: maybe write BSON, XML, etc... serializer for better performance.
@Deprecated
public class JsonMarshaller {

    private final ObjectMapper mapper = new ObjectMapper();

    public String serialize(Object x) {
        try {
            return mapper.writeValueAsString(x);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    public Optional<Object> deserialize(String x) {
        try {
            return Optional.of(mapper.readValue(x, Object.class));
        } catch (IOException e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }
}
