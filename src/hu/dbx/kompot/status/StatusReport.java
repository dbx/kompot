package hu.dbx.kompot.status;

import java.util.List;
import java.util.UUID;

public interface StatusReport {


    UUID getModuleIdentifier();

    String getEventGroup();

    String getMessageGroup();

    List<StatusItem> getItems();

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
         * Is the subsystem status acceptable?
         */
        boolean isOk();
    }
}
