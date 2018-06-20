package hu.dbx.kompot.consumer.async;

/**
 * Marks an enumeration as an event.
 *
 * @param <TReq> type of request payload
 */
public interface EventDescriptor<TReq> {

    // used for dispatch
    String getEventName();

    // used for deserialization of event data
    Class<? extends TReq> getRequestClass();

    static <T> EventDescriptor<T> of(String name, Class<? extends T> t) {
        return new EventDescriptor<T>() {
            @Override
            public String getEventName() {
                return name;
            }

            @Override
            public Class<? extends T> getRequestClass() {
                return t;
            }
        };
    }
}
