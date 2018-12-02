package hu.dbx.kompot.consumer.async;

import hu.dbx.kompot.events.Priority;

import static hu.dbx.kompot.events.Priority.LOW;

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
        return of(name, t, LOW);
    }

    static <T> EventDescriptor<T> of(String name, Class<? extends T> t, Priority priority) {
        return new EventDescriptor<T>() {
            @Override
            public String getEventName() {
                return name;
            }

            @Override
            public Class<? extends T> getRequestClass() {
                return t;
            }

            @Override
            public Priority getPriority() {
                return priority;
            }

            @Override
            public String toString() {
                return "<EventDescriptor of " + name + " with p=" + priority + " class=" + t + ">";
            }
        };
    }

    // used for dispatch
    default Priority getPriority() {
        return LOW;
    }
}
