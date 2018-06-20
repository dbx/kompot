package hu.dbx.kompot.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum LoggerUtils {
    ;

    /**
     * Returns a Logger instance for the class where this method is called from.
     */
    public static Logger getLogger() {
        try {
            final StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
            final Class<?> loggedClass = Class.forName(stackTrace[2].getClassName());
            return LoggerFactory.getLogger(loggedClass);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Could not create Logger instance", e);
        }
    }
}
