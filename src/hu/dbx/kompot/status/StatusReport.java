package hu.dbx.kompot.status;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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

    @JsonCreator
    public static StatusReport build(
            @JsonProperty("moduleIdentifier") UUID moduleIdentifier,
            @JsonProperty("eventGroup") String eventGroup,
            @JsonProperty("messageGroup") String messageGroup,
            @JsonProperty("lastHeartbeatTime") Date lastHeartbeatTime,
            @JsonProperty("items") List<StatusItem> items,

            @JsonProperty("registeredMethods")
                    Set<String> registeredMethods,
            @JsonProperty("registeredEvents")
                    Set<String> registeredEvents,
            @JsonProperty("registeredBroadcasts")
                    Set<String> registeredBroadcasts) {
        ConsumerIdentity cid = new ConsumerIdentity() {
            @Override
            public String getEventGroup() {
                return eventGroup;
            }

            @Override
            public String getIdentifier() {
                return moduleIdentifier.toString();
            }

            @Override
            public String getMessageGroup() {
                return messageGroup;
            }
        };
        return new StatusReport(cid, lastHeartbeatTime, items, registeredMethods, registeredEvents, registeredBroadcasts);
    }

    public UUID getModuleIdentifier() {
        return moduleIdentifier;
    }

    public String getEventGroup() {
        return eventGroup;
    }

    public String getMessageGroup() {
        return messageGroup;
    }

    /**
     * Timestamp of last time this module has written its status to the db.
     */
    public Date getLastHeartbeatTime() {
        return lastHeartbeatTime;
    }

    public List<StatusItem> getItems() {
        return unmodifiableList(items);
    }

    /**
     * Possibly empty list of all methods this module is listening to.
     */
    public Set<String> getRegisteredMethods() {
        return registeredMethods;
    }

    /**
     * Possibly empty lit of all event codes this module is listening to.
     */
    public Set<String> getRegisteredEvents() {
        return registeredEvents;
    }

    /**
     * List of all broadcast codes this module is module is listetning to.
     */
    public Set<String> getRegisteredBroadcasts() {
        return registeredBroadcasts;
    }

    public interface StatusItem {

        @JsonCreator
        static StatusItem build(
                @JsonProperty("name") String name,
                @JsonProperty("description") String description,
                @JsonProperty("errorMessage") String errorMessage,
                @JsonProperty("statusMessage") String statusMessage,
                @JsonProperty("ok") boolean ok) {
            return new StatusItemImpl(name, description, statusMessage, ok ? null : errorMessage);
        }

        /**
         * Short name of the status report.
         */
        String getName();

        /**
         * Short description of the status report.
         */
        String getDescription();

        /**
         * Detailed description of the actual status
         */
        String getStatusMessage();

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
