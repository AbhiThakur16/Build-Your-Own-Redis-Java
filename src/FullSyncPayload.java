import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class FullSyncPayload {

    /*
     * Master ki replication sequence jab
     * snapshot/full-sync state create hui.
     */
    private final long sequence;

    /*
     * Replica ko complete state rebuild
     * karne ke liye commands.
     *
     * Example:
     *
     * FLUSHALL
     * SET name Abhi
     * LPUSH fruits apple
     * HSET user age 20
     * ZADD leaderboard 100 Abhi
     */
    private final List<String> commands;


    public FullSyncPayload(
            long sequence,
            List<String> commands
    ) {

        this.sequence = sequence;

        if (commands == null) {

            this.commands =
                    Collections.emptyList();

        } else {

            this.commands =
                    Collections.unmodifiableList(
                            new ArrayList<>(
                                    commands
                            )
                    );
        }
    }


    // =========================================================
    // GET SEQUENCE
    // =========================================================

    public long getSequence() {

        return sequence;
    }


    // =========================================================
    // GET COMMANDS
    // =========================================================

    public List<String> getCommands() {

        return commands;
    }


    // =========================================================
    // COMMAND COUNT
    // =========================================================

    public int size() {

        return commands.size();
    }


    // =========================================================
    // EMPTY CHECK
    // =========================================================

    public boolean isEmpty() {

        return commands.isEmpty();
    }


    @Override
    public String toString() {

        return
                "FullSyncPayload{"
                        +
                "sequence="
                        + sequence
                        +
                ", commands="
                        + commands.size()
                        +
                '}';
    }
}