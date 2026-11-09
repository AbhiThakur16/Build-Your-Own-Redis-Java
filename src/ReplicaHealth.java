public final class ReplicaHealth {

    // =========================================================
    // STATUS
    // =========================================================

    public enum Status {
        ONLINE,
        SUSPECT,
        OFFLINE,
        UNKNOWN
    }

    // =========================================================
    // CONFIG
    // =========================================================

    private static final int SUSPECT_FAILURE_THRESHOLD = 2;
    private static final int OFFLINE_FAILURE_THRESHOLD = 3;

    // =========================================================
    // STATE
    // =========================================================

    private volatile Status status =
            Status.UNKNOWN;

    private volatile long lastSuccessfulHeartbeat =
            0;

    private volatile long lastFailedHeartbeat =
            0;

    private volatile long lastLatencyMillis =
            -1;

    private volatile int consecutiveFailures =
            0;

    private volatile long totalHeartbeats =
            0;

    private volatile long successfulHeartbeats =
            0;

    private volatile long failedHeartbeats =
            0;

    // =========================================================
    // SUCCESS
    // =========================================================

    public synchronized void recordSuccess(
            long latencyMillis
    ) {

        totalHeartbeats++;

        successfulHeartbeats++;

        consecutiveFailures =
                0;

        lastSuccessfulHeartbeat =
                System.currentTimeMillis();

        lastLatencyMillis =
                Math.max(
                        latencyMillis,
                        0
                );

        status =
                Status.ONLINE;
    }

    // =========================================================
    // FAILURE
    // =========================================================

    public synchronized void recordFailure() {

        totalHeartbeats++;

        failedHeartbeats++;

        consecutiveFailures++;

        lastFailedHeartbeat =
                System.currentTimeMillis();

        lastLatencyMillis =
                -1;

        if (
                consecutiveFailures
                        >=
                OFFLINE_FAILURE_THRESHOLD
        ) {

            status =
                    Status.OFFLINE;

        } else if (
                consecutiveFailures
                        >=
                SUSPECT_FAILURE_THRESHOLD
        ) {

            status =
                    Status.SUSPECT;
        }
    }

    // =========================================================
    // FORCE STATUS
    // =========================================================

    public synchronized void markOnline() {

        status =
                Status.ONLINE;

        consecutiveFailures =
                0;

        lastSuccessfulHeartbeat =
                System.currentTimeMillis();
    }

    public synchronized void markOffline() {

        status =
                Status.OFFLINE;

        if (
                consecutiveFailures
                        <
                OFFLINE_FAILURE_THRESHOLD
        ) {

            consecutiveFailures =
                    OFFLINE_FAILURE_THRESHOLD;
        }

        lastFailedHeartbeat =
                System.currentTimeMillis();
    }

    // =========================================================
    // GETTERS
    // =========================================================

    public Status getStatus() {

        return status;
    }

    public boolean isOnline() {

        return status
                ==
                Status.ONLINE;
    }

    public boolean isOffline() {

        return status
                ==
                Status.OFFLINE;
    }

    public boolean isSuspect() {

        return status
                ==
                Status.SUSPECT;
    }

    public long getLastSuccessfulHeartbeat() {

        return lastSuccessfulHeartbeat;
    }

    public long getLastFailedHeartbeat() {

        return lastFailedHeartbeat;
    }

    public long getLastLatencyMillis() {

        return lastLatencyMillis;
    }

    public int getConsecutiveFailures() {

        return consecutiveFailures;
    }

    public long getTotalHeartbeats() {

        return totalHeartbeats;
    }

    public long getSuccessfulHeartbeats() {

        return successfulHeartbeats;
    }

    public long getFailedHeartbeats() {

        return failedHeartbeats;
    }

    // =========================================================
    // UPTIME SINCE LAST SUCCESS
    // =========================================================

    public long getMillisSinceLastSuccess() {

        if (
                lastSuccessfulHeartbeat
                        ==
                0
        ) {

            return -1;
        }

        return System.currentTimeMillis()
                -
                lastSuccessfulHeartbeat;
    }

    // =========================================================
    // INFO
    // =========================================================

    public String getInfo() {

        return
                "health_status:"
                        + status

                        + "\nlast_latency_ms:"
                        + lastLatencyMillis

                        + "\nconsecutive_failures:"
                        + consecutiveFailures

                        + "\nlast_successful_heartbeat:"
                        + lastSuccessfulHeartbeat

                        + "\nlast_failed_heartbeat:"
                        + lastFailedHeartbeat

                        + "\ntotal_heartbeats:"
                        + totalHeartbeats

                        + "\nsuccessful_heartbeats:"
                        + successfulHeartbeats

                        + "\nfailed_heartbeats:"
                        + failedHeartbeats;
    }

    @Override
    public String toString() {

        return
                "ReplicaHealth{"
                        +
                "status="
                        + status
                        +
                ", latency="
                        + lastLatencyMillis
                        + "ms"
                        +
                ", consecutiveFailures="
                        + consecutiveFailures
                        +
                '}';
    }
}