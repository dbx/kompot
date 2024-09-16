package hu.dbx.kompot.impl.rabbit;

import java.io.IOException;

import static hu.dbx.kompot.core.SerializeHelper.getObjectMapper;

public final class RabbitHelper {

    private RabbitHelper() {
    }

    public static DataWrapper deserializeDataWrapper(final byte[] data) {
        try {
            return getObjectMapper().readValue(data, DataWrapper.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static MethodDataWrapper deserializeMethodDataWrapper(final byte[] data) {
        try {
            return getObjectMapper().readValue(data, MethodDataWrapper.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
