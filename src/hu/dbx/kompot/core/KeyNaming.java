package hu.dbx.kompot.core;

import java.util.UUID;

/**
 * Used to generate key names for different responsibilities.
 * Key names are used to store values in k-v database.
 */
public interface KeyNaming {

    /**
     * Key for the set of unprocessed events under a given group key.
     *
     * @return redis key for set of unprocessed events
     * @throws NullPointerException when eventGroupName is null or empty
     */
    String unprocessedEventsByGroupKey(String eventGroupName);

    /**
     * Key of a sorted set containing the event uuids that are being processed.
     *
     * @return redis key for set of processing events
     * @throws NullPointerException when eventGroupName is null or empty
     */
    String processingEventsByGroupKey(String eventGroupName);

    /**
     * Key of a sorted set containing the event uuids that are failed to process.
     *
     * @return redis key for set of failed event items
     * @throws NullPointerException when eventGroupName is null or empty
     */
    String failedEventsByGroupKey(String eventGroupName);


    /**
     * Egy rendezett halmaz kulcsa, ami alatt ott van a sikeresen feldolgozott esemenyek uuid listaja.
     *
     * @return redis key for set of failed event items
     * @throws NullPointerException when eventGroupName is null or empty
     */
    String processedEventsByGroupKey(String eventGroupName);

    /**
     * Place of event description.
     * <p>
     * Content such as event groups, name, comment, data comes under this key.
     *
     * @param eventId event identifier string
     * @return key of event historiy
     * @throws NullPointerException when parameter is empty
     */
    String eventDetailsKey(UUID eventId);

    /**
     * Returns all the event details
     */
    String allEventDetailsKey();

    /**
     * Event details for a given processor come here.
     *
     * @throws NullPointerException when any parameter is empty
     */
    String eventDetailsKey(String groupCode, UUID eventUuid);

    /**
     * Erre a csatornara kuldunk ertesitest ha erkezik valasz.
     *
     * @throws NullPointerException when method uuid is null
     */
    String getMessageResponseNotificationChannel(UUID messageUuid);

    /**
     * Method details are persisted here.
     *
     * @throws NullPointerException when method uuid is null
     */
    String methodDetailsKey(UUID methodUuid);

    /**
     * A key for a set for existing event group names. Used to enumerate events.
     *
     * @throws NullPointerException when method uuid is null
     */
    String eventGroupsKey();
}
