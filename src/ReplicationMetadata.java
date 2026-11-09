import java.io.*;
import java.nio.file.*;
import java.util.Properties;

public final class ReplicationMetadata {

    // =========================================================
    // STATE
    // =========================================================

    private static String fileName =
            "replication-meta.properties";

    private static String masterReplicationId =
            null;

    private static long masterEpoch =
            0;

    private static String upstreamReplicationId =
            null;

    private static long replicaSequence =
            0;

    private static int configuredPort =
            -1;

    private ReplicationMetadata() {
    }

    // =========================================================
    // CONFIGURE FILE PER SERVER PORT
    // =========================================================

    public static synchronized void configure(
            int port
    ) {

        configuredPort =
                port;

        fileName =
                "replication-meta-"
                        + port
                        + ".properties";

        load();
    }

    // =========================================================
    // INITIALIZE MASTER
    // =========================================================

    public static synchronized void initializeMaster() {

        /*
         * Existing valid identity is preserved across restart.
         */
        if (
                !ReplicationId.isValid(
                        masterReplicationId
                )
        ) {

            masterReplicationId =
                    ReplicationId.generate();

            if (masterEpoch <= 0) {

                masterEpoch =
                        1;
            }

            save();

            System.out.println(
                    "Generated new Master Replication ID."
            );

        } else {

            if (masterEpoch <= 0) {

                masterEpoch =
                        1;

                save();
            }

            System.out.println(
                    "Loaded existing Master Replication ID."
            );
        }

        System.out.println(
                "Master Replication ID = "
                        + ReplicationId.shortId(
                                masterReplicationId
                        )
                        + "..."
        );

        System.out.println(
                "Master Epoch = "
                        + masterEpoch
        );
    }

    // =========================================================
    // INITIALIZE REPLICA
    // =========================================================

    public static synchronized void initializeReplica() {

        System.out.println(
                "Replica replication metadata loaded."
        );

        System.out.println(
                "Upstream Replication ID = "
                        +
                        (
                                ReplicationId.isValid(
                                        upstreamReplicationId
                                )
                                        ?
                                ReplicationId.shortId(
                                        upstreamReplicationId
                                ) + "..."
                                        :
                                "none"
                        )
        );

        System.out.println(
                "Replica Sequence = "
                        + replicaSequence
        );
    }

    // =========================================================
    // MASTER ID
    // =========================================================

    public static synchronized String getMasterReplicationId() {

        return masterReplicationId;
    }

    public static synchronized void setMasterReplicationId(
            String replicationId
    ) {

        masterReplicationId =
                ReplicationId.normalize(
                        replicationId
                );

        save();
    }

    // =========================================================
    // MASTER EPOCH
    // =========================================================

    public static synchronized long getMasterEpoch() {

        return masterEpoch;
    }

    public static synchronized void setMasterEpoch(
            long epoch
    ) {

        if (epoch < 0) {

            throw new IllegalArgumentException(
                    "Master epoch cannot be negative"
            );
        }

        masterEpoch =
                epoch;

        save();
    }

    // =========================================================
    // NEW MASTER IDENTITY
    // =========================================================

    /*
     * Used when a replica is promoted to a new master.
     *
     * New master gets:
     *
     * 1. New replication ID
     * 2. Higher epoch
     * 3. No upstream master
     */
    public static synchronized String rotateMasterIdentity() {

        long nextEpoch =
                Math.max(
                        masterEpoch + 1,
                        1
                );

        masterReplicationId =
                ReplicationId.generate();

        masterEpoch =
                nextEpoch;

        upstreamReplicationId =
                null;

        save();

        System.out.println(
                "New Master Identity generated."
        );

        System.out.println(
                "Replication ID = "
                        + ReplicationId.shortId(
                                masterReplicationId
                        )
                        + "..."
        );

        System.out.println(
                "Master Epoch = "
                        + masterEpoch
        );

        return masterReplicationId;
    }

    // =========================================================
    // UPSTREAM REPLICATION ID
    // =========================================================

    public static synchronized String getUpstreamReplicationId() {

        return upstreamReplicationId;
    }

    public static synchronized void setUpstreamReplicationId(
            String replicationId
    ) {

        if (
                replicationId == null
                        ||
                replicationId.isBlank()
                        ||
                replicationId.equalsIgnoreCase(
                        "none"
                )
        ) {

            upstreamReplicationId =
                    null;

        } else {

            upstreamReplicationId =
                    ReplicationId.normalize(
                            replicationId
                    );
        }

        save();
    }

    public static synchronized boolean matchesUpstream(
            String replicationId
    ) {

        return ReplicationId.matches(
                upstreamReplicationId,
                replicationId
        );
    }

    // =========================================================
    // REPLICA SEQUENCE
    // =========================================================

    public static synchronized long getReplicaSequence() {

        return replicaSequence;
    }

    public static synchronized void setReplicaSequence(
            long sequence
    ) {

        replicaSequence =
                sequence;

        save();
    }

    // =========================================================
    // UPDATE REPLICA STATE TOGETHER
    // =========================================================

    public static synchronized void updateReplicaState(
            String upstreamId,
            long sequence
    ) {

        if (
                upstreamId == null
                        ||
                !ReplicationId.isValid(
                        upstreamId
                )
        ) {

            throw new IllegalArgumentException(
                    "Invalid upstream replication ID"
            );
        }

        upstreamReplicationId =
                ReplicationId.normalize(
                        upstreamId
                );

        replicaSequence =
                sequence;

        save();
    }

    // =========================================================
    // CLEAR UPSTREAM
    // =========================================================

    public static synchronized void clearUpstream() {

        upstreamReplicationId =
                null;

        save();
    }

    // =========================================================
    // SAVE
    // =========================================================

    public static synchronized boolean save() {

        Properties properties =
                new Properties();

        properties.setProperty(
                "port",
                String.valueOf(
                        configuredPort
                )
        );

        properties.setProperty(
                "master_repl_id",
                masterReplicationId == null
                        ?
                        ""
                        :
                        masterReplicationId
        );

        properties.setProperty(
                "master_epoch",
                String.valueOf(
                        masterEpoch
                )
        );

        properties.setProperty(
                "upstream_repl_id",
                upstreamReplicationId == null
                        ?
                        ""
                        :
                        upstreamReplicationId
        );

        properties.setProperty(
                "replica_sequence",
                String.valueOf(
                        replicaSequence
                )
        );

        Path target =
                Paths.get(
                        fileName
                );

        Path temporary =
                Paths.get(
                        fileName
                                + ".tmp"
                );

        try (
                OutputStream output =
                        Files.newOutputStream(
                                temporary,
                                StandardOpenOption.CREATE,
                                StandardOpenOption.TRUNCATE_EXISTING,
                                StandardOpenOption.WRITE
                        )
        ) {

            properties.store(
                    output,
                    "Java Redis Replication Metadata"
            );

        } catch (
                IOException e
        ) {

            System.out.println(
                    "Replication metadata save error: "
                            + e.getMessage()
            );

            return false;
        }

        try {

            /*
             * Try atomic replacement first.
             */
            Files.move(
                    temporary,
                    target,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE
            );

        } catch (
                AtomicMoveNotSupportedException e
        ) {

            try {

                Files.move(
                        temporary,
                        target,
                        StandardCopyOption.REPLACE_EXISTING
                );

            } catch (
                    IOException secondError
            ) {

                System.out.println(
                        "Replication metadata move error: "
                                + secondError.getMessage()
                );

                return false;
            }

        } catch (
                IOException e
        ) {

            System.out.println(
                    "Replication metadata move error: "
                            + e.getMessage()
            );

            return false;
        }

        return true;
    }

    // =========================================================
    // LOAD
    // =========================================================

    public static synchronized void load() {

        Path path =
                Paths.get(
                        fileName
                );

        if (
                !Files.exists(
                        path
                )
        ) {

            masterReplicationId =
                    null;

            masterEpoch =
                    0;

            upstreamReplicationId =
                    null;

            replicaSequence =
                    0;

            System.out.println(
                    "No replication metadata found for port "
                            + configuredPort
            );

            return;
        }

        Properties properties =
                new Properties();

        try (
                InputStream input =
                        Files.newInputStream(
                                path
                        )
        ) {

            properties.load(
                    input
            );

            // =============================================
            // MASTER REPLICATION ID
            // =============================================

            String storedMasterId =
                    properties.getProperty(
                            "master_repl_id",
                            ""
                    ).trim();

            if (
                    ReplicationId.isValid(
                            storedMasterId
                    )
            ) {

                masterReplicationId =
                        ReplicationId.normalize(
                                storedMasterId
                        );

            } else {

                masterReplicationId =
                        null;
            }

            // =============================================
            // MASTER EPOCH
            // =============================================

            masterEpoch =
                    parseLong(
                            properties.getProperty(
                                    "master_epoch"
                            ),
                            0
                    );

            // =============================================
            // UPSTREAM ID
            // =============================================

            String storedUpstreamId =
                    properties.getProperty(
                            "upstream_repl_id",
                            ""
                    ).trim();

            if (
                    ReplicationId.isValid(
                            storedUpstreamId
                    )
            ) {

                upstreamReplicationId =
                        ReplicationId.normalize(
                                storedUpstreamId
                        );

            } else {

                upstreamReplicationId =
                        null;
            }

            // =============================================
            // REPLICA SEQUENCE
            // =============================================

            replicaSequence =
                    parseLong(
                            properties.getProperty(
                                    "replica_sequence"
                            ),
                            0
                    );

            System.out.println(
                    "Replication metadata loaded for port "
                            + configuredPort
            );

        } catch (
                IOException e
        ) {

            System.out.println(
                    "Replication metadata load error: "
                            + e.getMessage()
            );

            masterReplicationId =
                    null;

            masterEpoch =
                    0;

            upstreamReplicationId =
                    null;

            replicaSequence =
                    0;
        }
    }

    // =========================================================
    // LONG PARSER
    // =========================================================

    private static long parseLong(
            String value,
            long defaultValue
    ) {

        if (
                value == null
                        ||
                value.isBlank()
        ) {

            return defaultValue;
        }

        try {

            return Long.parseLong(
                    value.trim()
            );

        } catch (
                NumberFormatException e
        ) {

            return defaultValue;
        }
    }

    // =========================================================
    // STATUS
    // =========================================================

    public static synchronized String getStatus() {

        return
                "metadata_file:"
                        + fileName

                        + "\nmaster_repl_id:"
                        +
                        (
                                masterReplicationId == null
                                        ?
                                "none"
                                        :
                                masterReplicationId
                        )

                        + "\nmaster_epoch:"
                        + masterEpoch

                        + "\nupstream_repl_id:"
                        +
                        (
                                upstreamReplicationId == null
                                        ?
                                "none"
                                        :
                                upstreamReplicationId
                        )

                        + "\nreplica_sequence:"
                        + replicaSequence;
    }

    // =========================================================
    // FILE NAME
    // =========================================================

    public static synchronized String getFileName() {

        return fileName;
    }
}