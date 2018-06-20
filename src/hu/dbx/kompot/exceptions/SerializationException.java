package hu.dbx.kompot.exceptions;

/**
 * An exception happened while serializing the object.
 */
public final class SerializationException extends Exception {
    private final Object source;

    public SerializationException(Object source, String message) {
        super(message);
        this.source = source;
    }

    public Object getSource() {
        return source;
    }
}
