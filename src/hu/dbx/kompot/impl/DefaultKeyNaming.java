package hu.dbx.kompot.impl;

import hu.dbx.kompot.core.KeyNaming;

import java.util.UUID;

/**
 * A prefixed key naming convention.
 * Starts every key with a prefix. Parts are separated by ":" (double colon).
 */
public final class DefaultKeyNaming implements KeyNaming {

    public static final String EVENT_DETAILS_PREFIX = ":ed:";
    private final String prefix;

    /**
     * Legyart egy peldanyt egy adott nemures prefixhez.
     */
    public static DefaultKeyNaming ofPrefix(String prefix) {
        return new DefaultKeyNaming(prefix);
    }

    private DefaultKeyNaming(String prefix) {
        if (prefix == null || prefix.trim().isEmpty()) {
            throw new IllegalArgumentException("Key Naming prefix nem lehet ures: '" + prefix + "'");
        } else {
            this.prefix = prefix;
        }
    }

    @Override
    public String unprocessedEventsByGroupKey(String eventGroupName) {
        if (isEmpty(eventGroupName)) {
            throw new IllegalArgumentException("Event group name must not be empty!");
        } else {
            return prefix + ":created:" + eventGroupName;
        }
    }

    @Override
    public String processingEventsByGroupKey(String eventGroupName) {
        if (isEmpty(eventGroupName)) {
            throw new IllegalArgumentException("Event group name must not be empty!");
        } else {
            return prefix + ":proc:" + eventGroupName;
        }
    }

    @Override
    public String failedEventsByGroupKey(String eventGroupName) {
        if (isEmpty(eventGroupName)) {
            throw new IllegalArgumentException("Event group name must not be empty!");
        } else {
            return prefix + ":failed:" + eventGroupName;
        }
    }

    @Override
    public String processedEventsByGroupKey(String eventGroupName) {
        if (isEmpty(eventGroupName)) {
            throw new IllegalArgumentException("Event group name must not be empty!");
        } else {
            return prefix + ":done:" + eventGroupName;
        }
    }

    @Override
    public String eventDetailsKey(UUID eventId) {
        if (eventId == null) {
            throw new NullPointerException("Event id must not be empty!");
        } else {
            return prefix + EVENT_DETAILS_PREFIX + eventId;
        }
    }

    @Override
    public String allEventDetailsKey() {
        return prefix + EVENT_DETAILS_PREFIX + "*";
    }

    @Override
    public String eventDetailsKey(String groupCode, UUID eventUuid) {
        if (isEmpty(groupCode)) {
            throw new NullPointerException("Group code is missing!");
        } else {
            return prefix + ":ee:" + groupCode + ":" + eventUuid;
        }
    }

    @Override
    public String methodDetailsKey(UUID methodUuid) {
        if (methodUuid == null) {
            throw new NullPointerException("Method UUID is empty!");
        } else {
            return prefix + ":md:" + methodUuid;
        }
    }

    @Override
    public String eventGroupsKey() {
        return prefix + ":grs";
    }

    @Override
    public String statusHolderKey(String groupName, String identifier) {
        return prefix + ":status:" + groupName + ":" + identifier;
    }

    @Override
    public String statusResponseKey() {
        return prefix + ":sr:" + UUID.randomUUID();
    }

    private boolean isEmpty(String s) {
        return null == s || s.isEmpty();
    }
}
