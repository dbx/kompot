package hu.dbx.kompot.status;

import hu.dbx.kompot.consumer.ConsumerIdentity;

import java.util.List;

public interface StatusReport {

    /**
     * The consumer giving the status report.
     */
    ConsumerIdentity getConsumerIdentity();

    /**
     * A list of status items for a given consumer or empty.
     * <p>
     * The ordering of the items is not guaranteed.
     */
    List<StatusItem> getItems();

    interface StatusItem {

        /**
         * Short name of the status report.
         * <p>
         * This should be a static constant value that does not change between invocations.
         */
        String getName();

        /**
         * Short description of the status report.
         * <p>
         * This should be a static constant value that does not change between invocations.
         */
        String getDescription();

        /**
         * Is the subsystem status acceptable?
         */
        boolean isOk();
    }
}
