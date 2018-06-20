package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.ConsumerIdentity;

import java.util.UUID;

/**
 * Default immutable implementation that assigns a random UUID on creation.
 */
@SuppressWarnings("unused")
public final class DefaultConsumerIdentity implements ConsumerIdentity {
    private final String identifier = UUID.randomUUID().toString();
    private final String eventGroup, messageGroup;

    /**
     * Sets event group and message group name from argument.
     */
    private DefaultConsumerIdentity(String eventGroup, String messageGroup) {
        this.eventGroup = eventGroup;
        this.messageGroup = messageGroup;
    }

    @SuppressWarnings("unused")
    public static ConsumerIdentity fromGroups(String eventGroup, String messageGroup) {
        return new DefaultConsumerIdentity(eventGroup, messageGroup);
    }

    @SuppressWarnings("unused")
    public static ConsumerIdentity groupGroup(String messageAndEventGroup) {
        return new DefaultConsumerIdentity(messageAndEventGroup, messageAndEventGroup);
    }

    @Override
    public String getEventGroup() {
        return eventGroup;
    }

    /**
     * Unique consumer identifier string. Must not change during runtime. May change between restarts.
     */
    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String getMessageGroup() {
        return messageGroup;
    }
}
