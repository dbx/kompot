package hu.dbx.kompot.report;

import java.util.UUID;

public interface EventUpdates {

    /**
     * Sets the event back to newly created state, if it is stuck
     *
     * @param eventUuid  identifier of event
     * @param eventGroup gruop name where events belong (not null!)
     * @throws IllegalArgumentException if the event does not exists or it is not in a resendable state
     */
    void resendEvent(UUID eventUuid, String eventGroup);

    /**
     * Removes the event
     *
     * @param eventUuid  identifier of event
     * @param eventGroup gruop name where events belong (not null!)
     * @throws IllegalArgumentException if the event does not exists
     */
    void removeEvent(UUID eventUuid, String eventGroup);

    //TODO: sendEvent???
    //TODO: updateEventData???
}
