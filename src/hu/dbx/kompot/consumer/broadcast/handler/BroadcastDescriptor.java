package hu.dbx.kompot.consumer.broadcast.handler;

/**
 * Supplies information about a broadcasted message.
 */
public interface BroadcastDescriptor<TReq> {

    /**
     * Broadcast code identifies the type of the broadcast.
     */
    String getBroadcastCode();

    /**
     * Type of payload of a broadcast message.
     * Used for deserializing broadcast messages.
     */
    Class<? extends TReq> getRequestClass();

    static <T> BroadcastDescriptor<T> of(String name, Class<? extends T> t) {
        return new BroadcastDescriptor<T>() {
            @Override
            public String getBroadcastCode() {
                return name;
            }

            @Override
            public Class<? extends T> getRequestClass() {
                return t;
            }
        };
    }
}
