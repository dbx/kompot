package hu.dbx.kompot.status;

import hu.dbx.kompot.consumer.ConsumerIdentity;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static java.util.Collections.unmodifiableList;

public final class StatusReport {

    private final UUID moduleIdentifier;
    private final String eventGroup;
    private final String messageGroup;
    private final Date lastHeartbeatTime;
    private final List<StatusItem> items;

    private final Set<String> registeredMethods;
    private final Set<String> registeredEvents;
    private final Set<String> registeredBroadcasts;

    public StatusReport(ConsumerIdentity id, Date lastHeartbeatTime, List<StatusItem> items, Set<String> registeredMethods, Set<String> registeredEvents, Set<String> registeredBroadcasts) {
        this.moduleIdentifier = UUID.fromString(id.getIdentifier());
        this.eventGroup = id.getEventGroup();
        this.messageGroup = id.getMessageGroup();
        this.lastHeartbeatTime = lastHeartbeatTime;
        this.items = items;
        this.registeredMethods = registeredMethods;
        this.registeredEvents = registeredEvents;
        this.registeredBroadcasts = registeredBroadcasts;
    }

    UUID getModuleIdentifier() {
        return moduleIdentifier;
    }

    String getEventGroup() {
        return eventGroup;
    }

    String getMessageGroup() {
        return messageGroup;
    }

    /**
     * Timestamp of last time this module has written its status to the db.
     */
    Date getLastHeartbeatTime() {
        return lastHeartbeatTime;
    }

    List<StatusItem> getItems() {
        return unmodifiableList(items);
    }

    /**
     * Possibly empty list of all methods this module is listening to.
     */
    Set<String> getRegisteredMethods() {
        return registeredMethods;
    }

    /**
     * Possibly empty lit of all event codes this module is listening to.
     */
    Set<String> getRegisteredEvents() {
        return registeredEvents;
    }

    /**
     * List of all broadcast codes this module is module is listetning to.
     */
    Set<String> getRegisteredBroadcasts() {
        return registeredBroadcasts;
    }


    public interface StatusItem {

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
