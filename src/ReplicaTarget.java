import java.util.Objects;

public final class ReplicaTarget {

    // =========================================================
    // BASIC CONFIGURATION
    // =========================================================

    private final String host;
    private final int port;

    // =========================================================
    // HEALTH MONITORING
    // =========================================================

    private final ReplicaHealth health;

    // =========================================================
    // RUNTIME STATUS
    // =========================================================

    private volatile boolean connected;

    private volatile long lastSyncedSequence;

    private volatile long lastReportedSequence;

    private volatile long commandsSent;

    private volatile long failedCommands;

    private volatile long reconnects;

    private volatile long fullSyncCount;

    private volatile long partialSyncCount;

    // =========================================================
    // CONSTRUCTOR
    // =========================================================

    public ReplicaTarget(
            String host,
            int port
    ) {

        if (
                host == null
                        ||
                host.isBlank()
        ) {

            throw new IllegalArgumentException(
                    "Replica host cannot be empty"
            );
        }

        if (
                port < 1
                        ||
                port > 65535
        ) {

            throw new IllegalArgumentException(
                    "Invalid replica port: "
                            + port
            );
        }

        this.host =
                host;

        this.port =
                port;

        this.health =
                new ReplicaHealth();

        this.connected =
                false;

        this.lastSyncedSequence =
                0;

        this.lastReportedSequence =
                0;

        this.commandsSent =
                0;

        this.failedCommands =
                0;

        this.reconnects =
                0;

        this.fullSyncCount =
                0;

        this.partialSyncCount =
                0;
    }

    // =========================================================
    // HOST
    // =========================================================

    public String getHost() {

        return host;
    }

    // =========================================================
    // PORT
    // =========================================================

    public int getPort() {

        return port;
    }

    // =========================================================
    // UNIQUE ID
    // =========================================================

    public String getId() {

        return host
                + ":"
                + port;
    }

    // =========================================================
    // HEALTH
    // =========================================================

    public ReplicaHealth getHealth() {

        return health;
    }

    public ReplicaHealth.Status getHealthStatus() {

        return health.getStatus();
    }

    public boolean isHealthy() {

        return health.isOnline();
    }

    // =========================================================
    // CONNECTION STATUS
    // =========================================================

    public boolean isConnected() {

        return connected;
    }

    public synchronized void setConnected(
            boolean connected
    ) {

        this.connected =
                connected;

        /*
         * Connection success/failure and heartbeat health
         * are related, but not exactly the same thing.
         *
         * A real heartbeat result will be recorded later.
         */
        if (connected) {

            health.markOnline();

        } else {

            health.markOffline();
        }
    }

    // =========================================================
    // LAST SYNCED SEQUENCE
    // =========================================================

    public long getLastSyncedSequence() {

        return lastSyncedSequence;
    }

    public void setLastSyncedSequence(
            long sequence
    ) {

        this.lastSyncedSequence =
                sequence;
    }

    // =========================================================
    // REPLICA REPORTED SEQUENCE
    // =========================================================

    public long getLastReportedSequence() {

        return lastReportedSequence;
    }

    public void setLastReportedSequence(
            long sequence
    ) {

        this.lastReportedSequence =
                sequence;
    }

    // =========================================================
    // COMMAND STATS
    // =========================================================

    public long getCommandsSent() {

        return commandsSent;
    }

    public synchronized void incrementCommandsSent() {

        commandsSent++;
    }

    public long getFailedCommands() {

        return failedCommands;
    }

    public synchronized void incrementFailedCommands() {

        failedCommands++;
    }

    // =========================================================
    // RECONNECT STATS
    // =========================================================

    public long getReconnects() {

        return reconnects;
    }

    public synchronized void incrementReconnects() {

        reconnects++;
    }

    // =========================================================
    // FULL SYNC STATS
    // =========================================================

    public long getFullSyncCount() {

        return fullSyncCount;
    }

    public synchronized void incrementFullSyncCount() {

        fullSyncCount++;
    }

    // =========================================================
    // PARTIAL SYNC STATS
    // =========================================================

    public long getPartialSyncCount() {

        return partialSyncCount;
    }

    public synchronized void incrementPartialSyncCount() {

        partialSyncCount++;
    }

    // =========================================================
    // HEARTBEAT HELPERS
    // =========================================================

    public void recordHeartbeatSuccess(
            long latencyMillis
    ) {

        health.recordSuccess(
                latencyMillis
        );
    }

    public void recordHeartbeatFailure() {

        health.recordFailure();
    }

    public long getHeartbeatLatencyMillis() {

        return health.getLastLatencyMillis();
    }

    public int getConsecutiveHeartbeatFailures() {

        return health.getConsecutiveFailures();
    }

    public long getSuccessfulHeartbeats() {

        return health.getSuccessfulHeartbeats();
    }

    public long getFailedHeartbeats() {

        return health.getFailedHeartbeats();
    }

    // =========================================================
    // STATUS
    // =========================================================

    public String getStatus() {

        return
                "replica:"
                        + getId()

                        + "\nconnected:"
                        + connected

                        + "\nhealth_status:"
                        + health.getStatus()

                        + "\nheartbeat_latency_ms:"
                        + health.getLastLatencyMillis()

                        + "\nheartbeat_failures:"
                        + health.getConsecutiveFailures()

                        + "\nlast_heartbeat_success:"
                        + health.getLastSuccessfulHeartbeat()

                        + "\nlast_heartbeat_failure:"
                        + health.getLastFailedHeartbeat()

                        + "\ntotal_heartbeats:"
                        + health.getTotalHeartbeats()

                        + "\nsuccessful_heartbeats:"
                        + health.getSuccessfulHeartbeats()

                        + "\nfailed_heartbeats:"
                        + health.getFailedHeartbeats()

                        + "\nlast_reported_sequence:"
                        + lastReportedSequence

                        + "\nlast_synced_sequence:"
                        + lastSyncedSequence

                        + "\ncommands_sent:"
                        + commandsSent

                        + "\nfailed_commands:"
                        + failedCommands

                        + "\nreconnects:"
                        + reconnects

                        + "\nfull_syncs:"
                        + fullSyncCount

                        + "\npartial_syncs:"
                        + partialSyncCount;
    }

    // =========================================================
    // EQUALITY
    // =========================================================

    @Override
    public boolean equals(
            Object object
    ) {

        if (this == object) {
            return true;
        }

        if (
                !(object instanceof ReplicaTarget)
        ) {

            return false;
        }

        ReplicaTarget other =
                (ReplicaTarget) object;

        return port == other.port
                &&
                host.equalsIgnoreCase(
                        other.host
                );
    }

    @Override
    public int hashCode() {

        return Objects.hash(
                host.toLowerCase(),
                port
        );
    }

    // =========================================================
    // PRINT
    // =========================================================

    @Override
    public String toString() {

        return
                "ReplicaTarget{"
                        +
                "host='"
                        + host
                        + '\''
                        +
                ", port="
                        + port
                        +
                ", connected="
                        + connected
                        +
                ", health="
                        + health.getStatus()
                        +
                ", latency="
                        + health.getLastLatencyMillis()
                        + "ms"
                        +
                ", lastSyncedSequence="
                        + lastSyncedSequence
                        +
                '}';
    }
}