package hu.dbx.kompot.exceptions;

public final class MessageErrorResultException extends Exception {

    private final String exceptionClass;

    public MessageErrorResultException(String message, String exceptionClass) {
        super(message);
        this.exceptionClass = exceptionClass;
    }

    public String getExceptionClass() {
        return exceptionClass;
    }
}
