package hu.dbx.kompot.events;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Priority of event sent.
 * <p>
 * High priority events will be processed earlier than Low priority events.
 */
public enum Priority {


    HIGH(7),
    LOW(6),
    BATCH(5),
    BATCH100(4),
    BATCH1000(3),
    BATCH10000(2),
    BATCH100000(1);

    /**
     * High priotity evts have higher score.
     */
    public final int score;

    Priority(int score) {
        this.score = score;
    }

    public static Priority getHighestPriority() {
        return Arrays.stream(Priority.values())
                .max(Comparator.comparing(it -> it.score))
                .orElse(Priority.HIGH);
    }

    public static Priority getPriorityByScore(int score) {
        return Arrays.stream(Priority.values())
                .filter(it -> it.score == score)
                .findFirst()
                .orElse(Priority.LOW);
    }

    public static Priority getPriorityByIndex(int idx) {
        if (idx < 100) {
            return Priority.BATCH;
        } else if (idx < 1000) {
            return Priority.BATCH100;
        } else if (idx < 10000) {
            return Priority.BATCH1000;
        } else if (idx < 100000) {
            return Priority.BATCH10000;
        } else {
            return Priority.BATCH100000;
        }
    }

}
