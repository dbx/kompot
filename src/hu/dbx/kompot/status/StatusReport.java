package hu.dbx.kompot.status;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

public interface StatusReport {

    UUID getModuleIdentifier();

    String getEventGroup();

    String getMessageGroup();

    /**
     * Timestamp of last time this module has written its status to the db.
     */
    Date getLastHeartbeatTime();

    List<StatusItem> getItems();

    /**
     * Possibly empty list of all methods this module is listening to.
     */
    Set<String> getRegisteredMethods();

    /**
     * Possibly empty lit of all event codes this module is listening to.
     */
    Set<String> getRegisteredEvents();

    /**
     * List of all broadcast codes this module is module is listetning to.
     */
    Set<String> getRegisteredBroadcasts();


    interface StatusItem {

        /**
         * Short name of the status report.
         */
        String getName();

        /**
         * Short description of the status report.
         */
        String getDescription();

        /**
         * Error description used if status is not ok
         */
        String getErrorMessage();

        /**
         * Is the subsystem status acceptable?
         */
        boolean isOk();
    }
}
