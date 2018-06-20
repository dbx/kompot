package hu.dbx.kompot.exceptions;

/**
 * An exception happened while deserializing the object.
 */
public final class DeserializationException extends Exception {
    private final String source;

    public DeserializationException(String source, String message) {
        super(message);
        this.source = source;
    }

    /**
     * The value we could not deserialize
     */
    public String getSource() {
        return source;
    }
}
