package hu.dbx.kompot.impl;

import hu.dbx.kompot.consumer.ConsumerIdentity;

import java.util.UUID;

/**
 * Default immutable implementation that assigns a random UUID on creation.
 */
public final class DefaultConsumerIdentity implements ConsumerIdentity {
    private final String identifier;
    private final String eventGroup, messageGroup;

    /**
     * Sets event group and message group name from argument.
     */
    private DefaultConsumerIdentity(String eventGroup, String messageGroup) {
        this.identifier = UUID.randomUUID().toString();
        this.eventGroup = eventGroup;
        this.messageGroup = messageGroup;
    }

    public DefaultConsumerIdentity(String identifier, String eventGroup, String messageGroup) {
        this.identifier = identifier;
        this.eventGroup = eventGroup;
        this.messageGroup = messageGroup;
    }

    public static ConsumerIdentity fromGroups(String eventGroup, String messageGroup) {
        return new DefaultConsumerIdentity(eventGroup, messageGroup);
    }

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
