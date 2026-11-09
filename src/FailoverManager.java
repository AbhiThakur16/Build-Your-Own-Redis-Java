import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public final class FailoverManager {

    // =========================================================
    // RESULT
    // =========================================================

    public static final class FailoverDecision {

        private final boolean possible;
        private final ReplicaTarget candidate;
        private final String reason;
        private final long masterSequence;
        private final long candidateSequence;
        private final long replicationLag;

        private FailoverDecision(
                boolean possible,
                ReplicaTarget candidate,
                String reason,
                long masterSequence,
                long candidateSequence,
                long replicationLag
        ) {

            this.possible =
                    possible;

            this.candidate =
                    candidate;

            this.reason =
                    reason;

            this.masterSequence =
                    masterSequence;

            this.candidateSequence =
                    candidateSequence;

            this.replicationLag =
                    replicationLag;
        }

        public static FailoverDecision success(
                ReplicaTarget candidate,
                long masterSequence
        ) {

            long candidateSequence =
                    candidate.getLastSyncedSequence();

            long lag =
                    Math.max(
                            masterSequence - candidateSequence,
                            0
                    );

            return new FailoverDecision(
                    true,
                    candidate,
                    "Healthy promotion candidate selected",
                    masterSequence,
                    candidateSequence,
                    lag
            );
        }

        public static FailoverDecision failure(
                String reason,
                long masterSequence
        ) {

            return new FailoverDecision(
                    false,
                    null,
                    reason,
                    masterSequence,
                    -1,
                    -1
            );
        }

        public boolean isPossible() {

            return possible;
        }

        public ReplicaTarget getCandidate() {

            return candidate;
        }

        public String getReason() {

            return reason;
        }

        public long getMasterSequence() {

            return masterSequence;
        }

        public long getCandidateSequence() {

            return candidateSequence;
        }

        public long getReplicationLag() {

            return replicationLag;
        }

        public String getCandidateId() {

            if (candidate == null) {

                return "none";
            }

            return candidate.getId();
        }

        @Override
        public String toString() {

            if (!possible) {

                return
                        "failover_possible:false"
                                + "\nreason:"
                                + reason
                                + "\nmaster_sequence:"
                                + masterSequence;
            }

            return
                    "failover_possible:true"
                            + "\ncandidate:"
                            + candidate.getId()
                            + "\nhealth:"
                            + candidate.getHealthStatus()
                            + "\nconnected:"
                            + candidate.isConnected()
                            + "\nmaster_sequence:"
                            + masterSequence
                            + "\ncandidate_sequence:"
                            + candidateSequence
                            + "\nreplication_lag:"
                            + replicationLag
                            + "\nheartbeat_latency_ms:"
                            + candidate.getHeartbeatLatencyMillis()
                            + "\nheartbeat_failures:"
                            + candidate.getConsecutiveHeartbeatFailures()
                            + "\nreason:"
                            + reason;
        }
    }

    // =========================================================
    // PRIVATE CONSTRUCTOR
    // =========================================================

    private FailoverManager() {
    }

    // =========================================================
    // SELECT FROM CURRENT REPLICATION MANAGER
    // =========================================================

    public static FailoverDecision selectBestCandidate() {

        List<ReplicaTarget> replicas =
                ReplicationManager.getReplicaTargets();

        long masterSequence =
                ReplicationManager.getCurrentSequence();

        return selectBestCandidate(
                replicas,
                masterSequence
        );
    }

    // =========================================================
    // SELECT BEST CANDIDATE
    // =========================================================

    public static FailoverDecision selectBestCandidate(
            List<ReplicaTarget> replicas,
            long masterSequence
    ) {

        if (
                replicas == null
                        ||
                replicas.isEmpty()
        ) {

            return FailoverDecision.failure(
                    "No replicas configured",
                    masterSequence
            );
        }

        List<ReplicaTarget> eligible =
                new ArrayList<>();

        for (
                ReplicaTarget replica :
                replicas
        ) {

            if (replica == null) {

                continue;
            }

            /*
             * Candidate must still have an active
             * replication connection.
             */
            if (!replica.isConnected()) {

                continue;
            }

            /*
             * Only ONLINE replicas are eligible.
             *
             * SUSPECT / OFFLINE / UNKNOWN are rejected.
             */
            if (
                    replica.getHealthStatus()
                            !=
                    ReplicaHealth.Status.ONLINE
            ) {

                continue;
            }

            /*
             * Invalid replication sequence means candidate
             * cannot safely be promoted.
             */
            if (
                    replica.getLastSyncedSequence()
                            <
                    0
            ) {

                continue;
            }

            eligible.add(
                    replica
            );
        }

        if (eligible.isEmpty()) {

            return FailoverDecision.failure(
                    "No healthy connected replicas available",
                    masterSequence
            );
        }

        // =====================================================
        // SORTING RULES
        // =====================================================
        //
        // Priority 1:
        // Highest synchronized sequence
        //
        // Priority 2:
        // Lowest consecutive heartbeat failures
        //
        // Priority 3:
        // Lowest heartbeat latency
        //
        // Priority 4:
        // Lowest port number for deterministic tie-breaking
        // =====================================================

        eligible.sort(
                Comparator
                        .comparingLong(
                                ReplicaTarget::getLastSyncedSequence
                        )
                        .reversed()

                        .thenComparingInt(
                                ReplicaTarget::
                                        getConsecutiveHeartbeatFailures
                        )

                        .thenComparingLong(
                                FailoverManager::
                                        normalizedLatency
                        )

                        .thenComparingInt(
                                ReplicaTarget::getPort
                        )
        );

        ReplicaTarget best =
                eligible.get(0);

        return FailoverDecision.success(
                best,
                masterSequence
        );
    }

    // =========================================================
    // NORMALIZED LATENCY
    // =========================================================

    private static long normalizedLatency(
            ReplicaTarget replica
    ) {

        long latency =
                replica.getHeartbeatLatencyMillis();

        /*
         * Unknown latency should be considered worse
         * than a known latency.
         */
        if (latency < 0) {

            return Long.MAX_VALUE;
        }

        return latency;
    }

    // =========================================================
    // FAILOVER SAFETY
    // =========================================================

    public static boolean isCandidateFullyCaughtUp(
            FailoverDecision decision
    ) {

        if (
                decision == null
                        ||
                !decision.isPossible()
        ) {

            return false;
        }

        return decision.getCandidateSequence()
                >=
                decision.getMasterSequence();
    }

    // =========================================================
    // NUMBER OF HEALTHY REPLICAS
    // =========================================================

    public static int countEligibleReplicas() {

        int count =
                0;

        for (
                ReplicaTarget replica :
                ReplicationManager.getReplicaTargets()
        ) {

            if (
                    replica != null
                            &&
                    replica.isConnected()
                            &&
                    replica.getHealthStatus()
                            ==
                    ReplicaHealth.Status.ONLINE
                            &&
                    replica.getLastSyncedSequence()
                            >=
                    0
            ) {

                count++;
            }
        }

        return count;
    }

    // =========================================================
    // STATUS REPORT
    // =========================================================

    public static String getFailoverStatus() {

        long masterSequence =
                ReplicationManager.getCurrentSequence();

        List<ReplicaTarget> replicas =
                ReplicationManager.getReplicaTargets();

        StringBuilder result =
                new StringBuilder();

        result.append(
                "master_sequence:"
        );

        result.append(
                masterSequence
        );

        result.append(
                "\nconfigured_replicas:"
        );

        result.append(
                replicas.size()
        );

        result.append(
                "\neligible_replicas:"
        );

        result.append(
                countEligibleReplicas()
        );

        int number =
                1;

        for (
                ReplicaTarget replica :
                replicas
        ) {

            result.append(
                    "\n\ncandidate_"
            );

            result.append(
                    number
            );

            result.append(
                    ":"
            );

            result.append(
                    replica.getId()
            );

            result.append(
                    "\ncandidate_"
            );

            result.append(
                    number
            );

            result.append(
                    "_connected:"
            );

            result.append(
                    replica.isConnected()
            );

            result.append(
                    "\ncandidate_"
            );

            result.append(
                    number
            );

            result.append(
                    "_health:"
            );

            result.append(
                    replica.getHealthStatus()
            );

            result.append(
                    "\ncandidate_"
            );

            result.append(
                    number
            );

            result.append(
                    "_sequence:"
            );

            result.append(
                    replica.getLastSyncedSequence()
            );

            result.append(
                    "\ncandidate_"
            );

            result.append(
                    number
            );

            result.append(
                    "_lag:"
            );

            long lag =
                    Math.max(
                            masterSequence
                                    -
                            replica.getLastSyncedSequence(),
                            0
                    );

            result.append(
                    lag
            );

            result.append(
                    "\ncandidate_"
            );

            result.append(
                    number
            );

            result.append(
                    "_latency_ms:"
            );

            result.append(
                    replica.getHeartbeatLatencyMillis()
            );

            number++;
        }

        FailoverDecision decision =
                selectBestCandidate(
                        replicas,
                        masterSequence
                );

        result.append(
                "\n\n"
        );

        result.append(
                decision
        );

        return result.toString();
    }
}