package hu.dbx.kompot.events;

/**
 * Priority of event sent.
 * <p>
 * High priority events will be processed earlier than Low priority events.
 */
public enum Priority {


    HIGH(2), LOW(1);

    /**
     * High priotity evts have higher score.
     */
    public final int score;

    Priority(int score) {
        this.score = score;
    }
}
