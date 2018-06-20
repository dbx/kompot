package hu.dbx.kompot.consumer;

/**
 * Used to uniquely identify consumer instances.
 * <p>
 * A szerver az esemenyek/uzenetek feldolgozoja.
 */
public interface ConsumerIdentity {

    /**
     * Event group is used to decide what events this node should be able to process.
     * One consumer listens to one event group only. One event may be sent to many event groups.
     * Must not change during runtime.
     *
     * @return not null event group id string
     */
    String getEventGroup();

    /**
     * Uniquely identifies consumer part of current running node.
     * Should be unique in the network. Uniqueness is not enforced and is used for debug pursposes only.
     * May be changed on restarts. May contain uuid string or sw/hw identifier.
     * Should not change during runtime.
     *
     * @return not null unique identity string
     */
    String getIdentifier();

    /**
     * Message group is used to decide whan messages this node should be able to process.
     * One consumer listens to one message gruop only. One method may be sent to exactly one (its own) message group.
     * Must not change during runtime.
     *
     * @return not null message group code string.
     */
    String getMessageGroup();
}
