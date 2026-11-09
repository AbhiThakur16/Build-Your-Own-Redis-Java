import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

public final class ReplicationBacklog {

    // =========================================================
    // BACKLOG ENTRY
    // =========================================================

    public static final class Entry {

        private final long sequence;
        private final String command;

        public Entry(
                long sequence,
                String command
        ) {

            this.sequence =
                    sequence;

            this.command =
                    command;
        }

        public long getSequence() {

            return sequence;
        }

        public String getCommand() {

            return command;
        }

        @Override
        public String toString() {

            return sequence
                    + " -> "
                    + command;
        }
    }

    // =========================================================
    // CONFIG
    // =========================================================

    /*
     * Educational backlog size.
     *
     * Master latest 1000 write commands
     * memory me rakhega.
     */
    private static final int MAX_BACKLOG_SIZE =
            1000;

    // =========================================================
    // STORAGE
    // =========================================================

    private static final ConcurrentSkipListMap<
            Long,
            String
            > backlog =
            new ConcurrentSkipListMap<>();

    private ReplicationBacklog() {
    }

    // =========================================================
    // ADD COMMAND
    // =========================================================

    public static synchronized void add(
            long sequence,
            String command
    ) {

        if (
                sequence <= 0
                        ||
                command == null
                        ||
                command.isBlank()
        ) {

            return;
        }

        backlog.put(
                sequence,
                command
        );

        trim();
    }

    // =========================================================
    // TRIM OLD COMMANDS
    // =========================================================

    private static void trim() {

        while (
                backlog.size()
                        >
                MAX_BACKLOG_SIZE
        ) {

            Long first =
                    backlog.firstKey();

            backlog.remove(
                    first
            );
        }
    }

    // =========================================================
    // CAN PARTIAL SYNC?
    // =========================================================

    public static synchronized boolean canServeFrom(
            long replicaSequence
    ) {

        if (backlog.isEmpty()) {

            /*
             * Sequence zero means replica has never
             * synchronized, so full sync required.
             */
            return replicaSequence == 0;
        }

        long oldest =
                backlog.firstKey();

        long newest =
                backlog.lastKey();

        /*
         * Replica ko next command chahiye:
         *
         * replicaSequence + 1
         *
         * Example:
         *
         * replica = 100
         * backlog oldest = 90
         * backlog newest = 120
         *
         * 101 available hai -> YES.
         */
        long nextRequired =
                replicaSequence + 1;

        /*
         * Replica already latest state par hai.
         */
        if (
                replicaSequence
                        ==
                newest
        ) {

            return true;
        }

        return nextRequired >= oldest
                &&
                nextRequired <= newest;
    }

    // =========================================================
    // GET COMMANDS AFTER SEQUENCE
    // =========================================================

    public static synchronized List<Entry> getAfter(
            long sequence
    ) {

        List<Entry> result =
                new ArrayList<>();

        if (backlog.isEmpty()) {

            return result;
        }

        for (
                var entry :
                backlog.tailMap(
                        sequence,
                        false
                ).entrySet()
        ) {

            result.add(
                    new Entry(
                            entry.getKey(),
                            entry.getValue()
                    )
            );
        }

        return result;
    }

    // =========================================================
    // GET OLDEST SEQUENCE
    // =========================================================

    public static synchronized long getOldestSequence() {

        if (backlog.isEmpty()) {

            return 0;
        }

        return backlog.firstKey();
    }

    // =========================================================
    // GET NEWEST SEQUENCE
    // =========================================================

    public static synchronized long getNewestSequence() {

        if (backlog.isEmpty()) {

            return 0;
        }

        return backlog.lastKey();
    }

    // =========================================================
    // SIZE
    // =========================================================

    public static synchronized int size() {

        return backlog.size();
    }

    // =========================================================
    // MAX SIZE
    // =========================================================

    public static int getMaxBacklogSize() {

        return MAX_BACKLOG_SIZE;
    }

    // =========================================================
    // CLEAR
    // =========================================================

    public static synchronized void clear() {

        backlog.clear();
    }

    // =========================================================
    // INFO
    // =========================================================

    public static synchronized String getInfo() {

        return
                "backlog_size:"
                        + backlog.size()
                        +
                "\nbacklog_max_size:"
                        + MAX_BACKLOG_SIZE
                        +
                "\nbacklog_oldest_sequence:"
                        + getOldestSequence()
                        +
                "\nbacklog_newest_sequence:"
                        + getNewestSequence();
    }
}