package hu.dbx.kompot.consumer.sync;

import java.util.Map;

/**
 * Marks an enumeration as an event.
 *
 * @param <TRequest>  Type of request
 * @param <TResponse> Type of response
 */
public interface MethodDescriptor<TRequest, TResponse> {

    long DEFAULT_TIMEOUT_MILLIS = 15_000L;

    /**
     * Timeout value in millisecs
     */
    default long getTimeout() {
        return DEFAULT_TIMEOUT_MILLIS;
    }

    /**
     * Used to dispatch on.
     */
    String getMethodName();

    /**
     * Used to group methods.
     * <p>
     * For example the consuming module-s kind is good: INDEXER, AUTH, POLICY, etc.
     */
    String getMethodGroupName();

    /**
     * Used for serialization.
     */
    Class<? extends TRequest> getRequestClass();

    /**
     * Used for deserialization.
     */
    Class<? extends TResponse> getResponseClass();

    static MethodDescriptor<Map, Map> ofName(final String methodGroupName, final String methodName) {
        return new MethodDescriptor<Map, Map>() {
            @Override
            public String getMethodName() {
                return methodName;
            }

            @Override
            public String getMethodGroupName() {
                return methodGroupName;
            }

            @Override
            public Class<Map> getRequestClass() {
                return Map.class;
            }

            @Override
            public Class<Map> getResponseClass() {
                return Map.class;
            }
        };
    }

    /**
     * Returns a copy of the method descriptor with the given timeout value.
     *
     * @throws IllegalArgumentException on negative timeout value.
     */
    @SuppressWarnings("unchecked")
    default MethodDescriptor<TRequest, TResponse> withTimeout(long millis) {
        if (millis <= 0) {
            throw new IllegalArgumentException("Timeout value must be positive: " + millis);
        }

        final MethodDescriptor other = this;
        return new MethodDescriptor<TRequest, TResponse>() {
            @Override
            public String getMethodName() {
                return other.getMethodName();
            }

            @Override
            public String getMethodGroupName() {
                return other.getMethodGroupName();
            }

            @Override
            public Class<TRequest> getRequestClass() {
                return other.getRequestClass();
            }

            @Override
            public Class<TResponse> getResponseClass() {
                return other.getResponseClass();
            }

            @Override
            public long getTimeout() {
                return millis;
            }
        };
    }

    @SuppressWarnings("unchecked")
    default <TReq> MethodDescriptor<TReq, TResponse> withRequestClass(final Class requestClass) {
        if (requestClass == null) {
            throw new IllegalArgumentException("Request class must not be null!");
        }

        final MethodDescriptor other = this;
        return new MethodDescriptor<TReq, TResponse>() {

            @Override
            public String getMethodName() {
                return other.getMethodName();
            }

            @Override
            public String getMethodGroupName() {
                return other.getMethodGroupName();
            }

            @Override
            public Class<TReq> getRequestClass() {
                return requestClass;
            }

            @Override
            public Class<TResponse> getResponseClass() {
                return other.getResponseClass();
            }

            @Override
            public long getTimeout() {
                return other.getTimeout();
            }
        };
    }

    @SuppressWarnings("unchecked")
    default <TRes> MethodDescriptor<TRequest, TRes> withResponseClass(final Class responseClass) {
        if (responseClass == null) {
            throw new IllegalArgumentException("Response class must not be null!");
        }

        final MethodDescriptor other = this;
        return new MethodDescriptor<TRequest, TRes>() {

            @Override
            public String getMethodName() {
                return other.getMethodName();
            }

            @Override
            public String getMethodGroupName() {
                return other.getMethodGroupName();
            }

            @Override
            public Class<TRequest> getRequestClass() {
                return other.getRequestClass();
            }

            @Override
            public Class<TRes> getResponseClass() {
                return responseClass;
            }

            @Override
            public long getTimeout() {
                return other.getTimeout();
            }
        };
    }
}
