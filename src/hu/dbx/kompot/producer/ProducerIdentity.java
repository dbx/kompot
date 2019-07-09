package hu.dbx.kompot.producer;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.UUID;

/**
 * Egyertelmuen meghataroz egy klienst. A kliens az esemenyek/uzenetek forrasa.
 */
public interface ProducerIdentity {

    class RandomUuidIdentity implements ProducerIdentity {

        private String uuid = UUID.randomUUID().toString();

        public String getIdentifier() {
            return uuid;
        }
    }

    class CustomIdentity implements ProducerIdentity {

        private final String id;

        public CustomIdentity(String id) {
            this.id = id;
        }

        public String getIdentifier() {
            return id;
        }
    }

    class DetailedIdentity implements ProducerIdentity {

        String shortUuid = RandomStringUtils.randomAlphanumeric(8);

        private String id;

        public DetailedIdentity(String moduleName) {
            this(moduleName, null);
        }

        public DetailedIdentity(String moduleName, String moduleVersion) {
            if (StringUtils.isBlank(moduleName))
                throw new IllegalArgumentException("Module name should not be null");

            final StringBuilder builder = new StringBuilder();
            builder.append(moduleName);
            builder.append("-");
            if (StringUtils.isNotBlank(moduleVersion)) {
                builder.append(moduleVersion);
                builder.append("-");
            }
            builder.append(shortUuid);
            id = builder.toString();
        }


        public String getIdentifier() {
            return id;
        }
    }


    /**
     * Unique identifier of the current producer instance.
     * <p>
     * May change during restarts. Must not change during runtime.
     * Might contain UUID string, program name, version, hash, etc.
     * <p>
     * Used for debugging purposes only.
     *
     * @return unique identifier string.
     */
    String getIdentifier();
}
