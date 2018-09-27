package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.async.EventDescriptor;
import hu.dbx.kompot.consumer.async.EventFrame;
import hu.dbx.kompot.consumer.sync.MethodDescriptor;
import hu.dbx.kompot.consumer.sync.MethodRequestFrame;
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

    /**
     * Prints methodDescriptor details to debug log.
     */
    public static void debugMethodDescriptor(final Logger logger, final MethodDescriptor methodDescriptor) {
        if (methodDescriptor == null) {
            logger.debug("Missing method descriptor.");
        } else {
            logger.debug("Method {}.{} with timeout={}", methodDescriptor.getMethodGroupName(), methodDescriptor.getMethodName(), methodDescriptor.getTimeout());
            logger.debug("requestClass={} responseClass={}", methodDescriptor.getRequestClass(), methodDescriptor.getResponseClass());
        }
    }

    public static void debugMethodFrame(final Logger logger, final MethodRequestFrame frame) {
        if (frame == null) {
            logger.debug("Missing method frame.");
        } else {
            logger.debug("Method frame id={} sender={}", frame.getIdentifier(), frame.getSourceIdentifier());
            logger.debug("meta={}", frame.getMetaData());
            LoggerUtils.debugMethodDescriptor(logger, frame.getMethodMarker());
        }
    }

    /**
     * Prints eventDescriptor details to debug log.
     */
    public static void debugEventDescriptor(final Logger logger, final EventDescriptor eventDescriptor) {
        if (eventDescriptor == null) {
            logger.debug("Missing event descriptor.");
        } else {
            logger.debug("Event {} with priority={}", eventDescriptor.getEventName(), eventDescriptor.getPriority());
            logger.debug("requestClass={}", eventDescriptor.getRequestClass());
        }
    }

    public static void debugEventFrame(final Logger logger, final EventFrame eventFrame) {
        if (eventFrame == null) {
            logger.debug("Missing event frame.");
        } else {
            logger.debug("Event frame uuid={}", eventFrame.getIdentifier());
            debugEventDescriptor(logger, eventFrame.getEventMarker());
        }
    }
}
