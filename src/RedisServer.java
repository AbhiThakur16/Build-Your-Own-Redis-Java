import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class RedisServer {

    // =========================================================
    // ROLE / CONFIG
    // =========================================================

    private static volatile NodeRole serverRole = NodeRole.MASTER;

    private static int PORT = 6379;

    private static final List<ReplicaTarget> configuredReplicas =
            new ArrayList<>();

    private static String DATA_FILE =
            "redis-data-6379.bin";

    private static final String REPLICATION_TOKEN =
            System.getenv().getOrDefault(
                    "JAVA_REDIS_REPLICATION_TOKEN",
                    "redis-replica-123"
            );

    // =========================================================
    // TYPES
    // =========================================================

    private enum KeyType {
        NONE,
        STRING,
        LIST,
        SET,
        HASH,
        ZSET
    }

    private static final String WRONGTYPE_ERROR =
            "ERROR: WRONGTYPE Operation against a key holding the wrong kind of value";

    // =========================================================
    // LOCKS
    // =========================================================

    private static final Object COMMAND_LOCK =
            new Object();

    private static final Object PERSISTENCE_LOCK =
            new Object();

    private static volatile boolean replayingAof = false;

    // =========================================================
    // REPLICA FULL SYNC STATE
    // =========================================================

    private static volatile boolean replicaFullSyncInProgress =
            false;

    private static volatile String replicaFullSyncReplicationId =
            null;

    private static volatile long replicaFullSyncEpoch =
            0;

    private static volatile long replicaFullSyncTargetSequence =
            0;

    // =========================================================
    // REPLICA PARTIAL SYNC STATE
    // =========================================================

    private static volatile boolean replicaPartialSyncInProgress =
            false;

    private static volatile String replicaPartialSyncReplicationId =
            null;

    private static volatile long replicaPartialSyncEpoch =
            0;

    private static volatile long replicaPartialSyncStartSequence =
            0;

    private static volatile long replicaPartialSyncEndSequence =
            0;

    // =========================================================
    // REPLICA APPLIED OFFSET
    // =========================================================

    private static final AtomicLong replicaAppliedSequence =
            new AtomicLong(0);

    // =========================================================
    // MEMORY / LRU
    // =========================================================

    private static final int DEFAULT_MAX_KEYS = 100;

    private static volatile int maxKeys =
            DEFAULT_MAX_KEYS;

    private static final ConcurrentHashMap<String, Long> accessOrder =
            new ConcurrentHashMap<>();

    private static final AtomicLong accessCounter =
            new AtomicLong(0);

    private static final AtomicLong evictedKeys =
            new AtomicLong(0);

    // =========================================================
    // DATA
    // =========================================================

    private static final ConcurrentHashMap<String, String> database =
            new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<String, Long> expirations =
            new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<
            String,
            ConcurrentLinkedDeque<String>
            > lists =
            new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<
            String,
            Set<String>
            > sets =
            new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<
            String,
            ConcurrentHashMap<String, String>
            > hashes =
            new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<
            String,
            SortedSetStore
            > sortedSets =
            new ConcurrentHashMap<>();

    // =========================================================
    // THREADS
    // =========================================================

    private static final ExecutorService threadPool =
            Executors.newFixedThreadPool(20);

    private static final ScheduledExecutorService expirationCleaner =
            Executors.newSingleThreadScheduledExecutor();

    // =========================================================
    // SNAPSHOT
    // =========================================================

    private static class Snapshot implements Serializable {

        private static final long serialVersionUID = 1L;

        Map<String, String> strings = new HashMap<>();

        Map<String, Long> expirations = new HashMap<>();

        Map<String, List<String>> lists = new HashMap<>();

        Map<String, Set<String>> sets = new HashMap<>();

        Map<String, Map<String, String>> hashes = new HashMap<>();

        Map<String, Map<String, Double>> sortedSets = new HashMap<>();

        Map<String, Long> accessOrder = new HashMap<>();

        int maxKeys;

        long accessCounter;

        long evictedKeys;

        long replicaAppliedSequence;
    }

    // =========================================================
    // MAIN
    // =========================================================

    public static void main(String[] args) {

        try {

            configureServer(args);

        } catch (Exception e) {

            System.out.println(
                    "Server configuration error: "
                            + e.getMessage()
            );

            System.out.println(
                    "Master : java RedisServer master 6379"
            );

            System.out.println(
                    "Replica: java RedisServer replica 6380"
            );

            return;
        }

        DATA_FILE =
                "redis-data-"
                        + PORT
                        + ".bin";

        AofManager.configure(
                "appendonly-"
                        + PORT
                        + ".aof"
        );

        // Each server has its own replication metadata file.
        ReplicationMetadata.configure(
                PORT
        );

        System.out.println();
        System.out.println(
                "Starting Java Redis Server..."
        );

        System.out.println(
                "Server Role = "
                        + serverRole
        );

        System.out.println(
                "Server Port = "
                        + PORT
        );

        // =====================================================
        // LOAD PERSISTED DATABASE
        // =====================================================

        replayingAof = true;

        loadSnapshot();

        replayAof();

        cleanExpiredKeys();

        replayingAof = false;

        // =====================================================
        // INITIALIZE REPLICATION IDENTITY
        // =====================================================

        initializeReplicationIdentity();

        startExpirationCleaner();

        if (serverRole.isMaster()) {

            startMasterReplication();
        }

        try (
                ServerSocket serverSocket =
                        new ServerSocket(PORT)
        ) {

            printStartupInformation();

            while (true) {

                Socket clientSocket =
                        serverSocket.accept();

                System.out.println(
                        "Client connected: "
                                + clientSocket
                                .getRemoteSocketAddress()
                );

                threadPool.submit(
                        () -> handleClient(
                                clientSocket
                        )
                );
            }

        } catch (IOException e) {

            System.out.println(
                    "Server error: "
                            + e.getMessage()
            );

        } finally {

            if (
                    serverRole.isMaster()
                            &&
                    ReplicationManager.isEnabled()
            ) {

                ReplicationManager.stop();
            }

            expirationCleaner.shutdownNow();

            threadPool.shutdown();
        }
    }

    // =========================================================
    // REPLICATION IDENTITY INITIALIZATION
    // =========================================================

    private static void initializeReplicationIdentity() {

        if (serverRole.isMaster()) {

            ReplicationMetadata.initializeMaster();

            /*
             * Important for promoted masters.
             *
             * A replica may already have sequence 25 when it is
             * restarted as a master. We must not reset that
             * sequence back to zero.
             */
            long persisted =
                    ReplicationMetadata
                            .getReplicaSequence();

            long databaseSequence =
                    Math.max(
                            replicaAppliedSequence.get(),
                            0
                    );

            long safeSequence =
                    Math.max(
                            persisted,
                            databaseSequence
                    );

            ReplicationMetadata.setReplicaSequence(
                    safeSequence
            );

            System.out.println(
                    "Master starting sequence = "
                            + safeSequence
            );

        } else {

            ReplicationMetadata.initializeReplica();

            /*
             * Database/AOF state is authoritative for the
             * actual applied sequence.
             */
            ReplicationMetadata.setReplicaSequence(
                    replicaAppliedSequence.get()
            );
        }
    }

    // =========================================================
    // STARTUP INFO
    // =========================================================

    private static void printStartupInformation() {

        System.out.println();
        System.out.println(
                "Redis Server running on port "
                        + PORT
        );

        System.out.println(
                "Role = "
                        + serverRole
        );

        System.out.println(
                "Persistence File = "
                        + DATA_FILE
        );

        System.out.println(
                "AOF File = "
                        + AofManager.getFileName()
        );

        System.out.println(
                "Replication Metadata = "
                        + ReplicationMetadata.getFileName()
        );

        System.out.println(
                "MAX_KEYS = "
                        + maxKeys
        );

        System.out.println(
                "Authentication = "
                        +
                        (
                                AuthManager.isAuthenticationEnabled()
                                        ?
                                        "enabled"
                                        :
                                        "disabled"
                        )
        );

        System.out.println(
                "RESP = enabled"
        );

        System.out.println(
                "Type Safety = enabled"
        );

        System.out.println(
                "Transactions = enabled"
        );

        System.out.println(
                "WATCH = enabled"
        );

        System.out.println(
                "Pub/Sub = enabled"
        );

        System.out.println(
                "Snapshot + AOF = enabled"
        );

        System.out.println(
                "LRU Eviction = enabled"
        );

        System.out.println(
                "Full Resynchronization = enabled"
        );

        System.out.println(
                "Partial Resynchronization = enabled"
        );

        System.out.println(
                "Replication Backlog = enabled"
        );

        System.out.println(
                "Multiple Replicas = enabled"
        );

        System.out.println(
                "Heartbeat Monitoring = enabled"
        );

        System.out.println(
                "Failover = enabled"
        );

        System.out.println(
                "Replication ID Safety = enabled"
        );

        if (serverRole.isMaster()) {

            System.out.println(
                    "Master Replication ID = "
                            +
                            ReplicationMetadata
                                    .getMasterReplicationId()
            );

            System.out.println(
                    "Master Epoch = "
                            +
                            ReplicationMetadata
                                    .getMasterEpoch()
            );

            System.out.println(
                    "Master Sequence = "
                            +
                            ReplicationMetadata
                                    .getReplicaSequence()
            );

            System.out.println(
                    "Configured Replicas = "
                            + configuredReplicas.size()
            );

            for (
                    ReplicaTarget target :
                    configuredReplicas
            ) {

                System.out.println(
                        "  -> "
                                + target.getId()
                );
            }

        } else {

            System.out.println(
                    "Upstream Replication ID = "
                            +
                            (
                                    ReplicationMetadata
                                            .getUpstreamReplicationId()
                                            ==
                                    null
                                            ?
                                    "none"
                                            :
                                    ReplicationMetadata
                                            .getUpstreamReplicationId()
                            )
            );

            System.out.println(
                    "Upstream Epoch = "
                            +
                            ReplicationMetadata
                                    .getMasterEpoch()
            );

            System.out.println(
                    "Replica Applied Sequence = "
                            + replicaAppliedSequence.get()
            );
        }

        System.out.println();
        System.out.println(
                "Waiting for clients..."
        );
    }

    // =========================================================
    // SERVER CONFIG
    // =========================================================

    private static void configureServer(
            String[] args
    ) {

        configuredReplicas.clear();

        if (args.length == 0) {

            serverRole =
                    NodeRole.MASTER;

            PORT =
                    6379;

            addDefaultReplicas();

            return;
        }

        serverRole =
                NodeRole.fromString(
                        args[0]
                );

        if (serverRole.isMaster()) {

            PORT =
                    args.length >= 2
                            ?
                            Integer.parseInt(
                                    args[1]
                            )
                            :
                            6379;

            if (args.length < 3) {

                addDefaultReplicas();

            } else {

                for (
                        int i = 2;
                        i < args.length;
                        i++
                ) {

                    addReplicaArgument(
                            args[i]
                    );
                }
            }

        } else {

            PORT =
                    args.length >= 2
                            ?
                            Integer.parseInt(
                                    args[1]
                            )
                            :
                            6380;
        }
    }

    // =========================================================
    // DEFAULT REPLICAS
    // =========================================================

    private static void addDefaultReplicas() {

        int[] ports = {
                6380,
                6381,
                6382
        };

        for (int port : ports) {

            /*
             * Important when a promoted node such as 6380 is
             * restarted as MASTER. Never replicate to itself.
             */
            if (port == PORT) {
                continue;
            }

            configuredReplicas.add(
                    new ReplicaTarget(
                            "localhost",
                            port
                    )
            );
        }
    }

    // =========================================================
    // PROMOTION TARGETS
    // =========================================================

    private static void configurePromotionReplicas() {

        configuredReplicas.clear();

        addDefaultReplicas();
    }

    // =========================================================
    // CUSTOM REPLICA
    // =========================================================

    private static void addReplicaArgument(
            String argument
    ) {

        if (
                argument == null
                        ||
                argument.isBlank()
        ) {

            return;
        }

        String host =
                "localhost";

        int port;

        if (argument.contains(":")) {

            String[] split =
                    argument.split(
                            ":",
                            2
                    );

            host =
                    split[0];

            port =
                    Integer.parseInt(
                            split[1]
                    );

        } else {

            port =
                    Integer.parseInt(
                            argument
                    );
        }

        if (
                host.equalsIgnoreCase(
                        "localhost"
                )
                        &&
                port == PORT
        ) {

            System.out.println(
                    "Ignoring self replica target: "
                            + host
                            + ":"
                            + port
            );

            return;
        }

        configuredReplicas.add(
                new ReplicaTarget(
                        host,
                        port
                )
        );
    }

    // =========================================================
    // MASTER REPLICATION
    // =========================================================

    private static void startMasterReplication() {

        ReplicationManager.setFullSyncProvider(
                RedisServer::createFullSyncPayload
        );

        ReplicationManager.startMaster(
                configuredReplicas
        );
    }

    // =========================================================
    // SAFE LOG
    // =========================================================

    private static String safeCommandForLog(
            String command
    ) {

        String operation =
                getOperation(
                        command
                );

        if (
                operation.equals("AUTH")
                        ||
                operation.equals("REPLICAHELLO")
        ) {

            return operation
                    + " [REDACTED]";
        }

        return command;
    }

    // =========================================================
    // CLIENT HANDLER
    // =========================================================

    private static void handleClient(
            Socket clientSocket
    ) {

        boolean authenticated =
                !AuthManager
                        .isAuthenticationEnabled();

        boolean replicationConnection =
                false;

        boolean inTransaction =
                false;

        List<String> transactionQueue =
                new ArrayList<>();

        Map<String, Long> watchedKeys =
                new HashMap<>();

        PubSubManager.Subscriber subscriber =
                null;

        try {

            InputStream input =
                    clientSocket.getInputStream();

            OutputStream output =
                    clientSocket.getOutputStream();

            subscriber =
                    PubSubManager.createSubscriber(
                            output
                    );

            while (true) {

                String command =
                        RespProtocol.readCommand(
                                input
                        );

                if (command == null) {
                    break;
                }

                command =
                        command.trim();

                if (command.isEmpty()) {

                    RespProtocol.writeError(
                            output,
                            "Empty command"
                    );

                    continue;
                }

                String operation =
                        getOperation(
                                command
                        );

                System.out.println(
                        Thread.currentThread().getName()
                                + " -> "
                                +
                                safeCommandForLog(
                                        command
                                )
                );

                // =================================================
                // AUTH
                // =================================================

                if (operation.equals("AUTH")) {

                    String[] p =
                            command.split(
                                    "\\s+",
                                    2
                            );

                    if (p.length < 2) {

                        RespProtocol.writeError(
                                output,
                                "AUTH requires password"
                        );

                        continue;
                    }

                    if (
                            AuthManager.authenticate(
                                    p[1]
                            )
                    ) {

                        authenticated =
                                true;

                        RespProtocol.writeSimpleString(
                                output,
                                "OK"
                        );

                    } else {

                        authenticated =
                                false;

                        RespProtocol.writeError(
                                output,
                                "invalid password"
                        );
                    }

                    continue;
                }

                if (!authenticated) {

                    RespProtocol.writeError(
                            output,
                            "NOAUTH Authentication required"
                    );

                    continue;
                }

                // =================================================
                // REPLICAHELLO
                // =================================================

                if (
                        operation.equals(
                                "REPLICAHELLO"
                        )
                ) {

                    if (!serverRole.isReplica()) {

                        RespProtocol.writeError(
                                output,
                                "Not a replica server"
                        );

                        continue;
                    }

                    String[] p =
                            command.split(
                                    "\\s+",
                                    2
                            );

                    if (
                            p.length < 2
                                    ||
                            !REPLICATION_TOKEN.equals(
                                    p[1]
                            )
                    ) {

                        RespProtocol.writeError(
                                output,
                                "Invalid replication token"
                        );

                        continue;
                    }

                    replicationConnection =
                            true;

                    RespProtocol.writeSimpleString(
                            output,
                            "OK"
                    );

                    System.out.println(
                            "Master replication connection authenticated."
                    );

                    continue;
                }

                // =================================================
                // REPLGETSTATE
                // =================================================

                if (
                        operation.equals(
                                "REPLGETSTATE"
                        )
                ) {

                    if (
                            !serverRole.isReplica()
                                    ||
                            !replicationConnection
                    ) {

                        RespProtocol.writeError(
                                output,
                                "Replication connection required"
                        );

                        continue;
                    }

                    String upstreamId =
                            ReplicationMetadata
                                    .getUpstreamReplicationId();

                    if (
                            upstreamId == null
                                    ||
                            !ReplicationId.isValid(
                                    upstreamId
                            )
                    ) {

                        upstreamId =
                                "none";
                    }

                    RespProtocol.writeArray(
                            output,
                            Arrays.asList(
                                    upstreamId,
                                    String.valueOf(
                                            ReplicationMetadata
                                                    .getMasterEpoch()
                                    ),
                                    String.valueOf(
                                            replicaAppliedSequence
                                                    .get()
                                    )
                            )
                    );

                    continue;
                }

                // =================================================
                // LEGACY REPLGETSEQ
                // =================================================

                if (
                        operation.equals(
                                "REPLGETSEQ"
                        )
                ) {

                    if (
                            !serverRole.isReplica()
                                    ||
                            !replicationConnection
                    ) {

                        RespProtocol.writeError(
                                output,
                                "Replication connection required"
                        );

                        continue;
                    }

                    RespProtocol.writeInteger(
                            output,
                            replicaAppliedSequence.get()
                    );

                    continue;
                }

                // =================================================
                // FULL SYNC BEGIN
                //
                // REPLFULLSYNCBEGIN replId epoch sequence
                // =================================================

                if (
                        operation.equals(
                                "REPLFULLSYNCBEGIN"
                        )
                ) {

                    if (
                            !serverRole.isReplica()
                                    ||
                            !replicationConnection
                    ) {

                        RespProtocol.writeError(
                                output,
                                "Replication connection required"
                        );

                        continue;
                    }

                    String[] p =
                            command.split("\\s+");

                    if (p.length < 4) {

                        RespProtocol.writeError(
                                output,
                                "REPLFULLSYNCBEGIN requires replId epoch sequence"
                        );

                        continue;
                    }

                    String replId =
                            p[1];

                    long epoch;
                    long sequence;

                    try {

                        if (
                                !ReplicationId.isValid(
                                        replId
                                )
                        ) {

                            throw new IllegalArgumentException();
                        }

                        epoch =
                                Long.parseLong(
                                        p[2]
                                );

                        sequence =
                                Long.parseLong(
                                        p[3]
                                );

                        if (epoch <= 0) {

                            throw new IllegalArgumentException();
                        }

                    } catch (Exception e) {

                        RespProtocol.writeError(
                                output,
                                "Invalid full sync metadata"
                        );

                        continue;
                    }

                    synchronized (COMMAND_LOCK) {

                        replicaPartialSyncInProgress =
                                false;

                        replicaPartialSyncReplicationId =
                                null;

                        replicaPartialSyncEpoch =
                                0;

                        replicaPartialSyncStartSequence =
                                0;

                        replicaPartialSyncEndSequence =
                                0;

                        replicaFullSyncInProgress =
                                true;

                        replicaFullSyncReplicationId =
                                ReplicationId.normalize(
                                        replId
                                );

                        replicaFullSyncEpoch =
                                epoch;

                        replicaFullSyncTargetSequence =
                                sequence;

                        /*
                         * -1 means database state is temporarily
                         * unsafe until FULLSYNCEND succeeds.
                         */
                        replicaAppliedSequence.set(
                                -1
                        );

                        ReplicationMetadata.setMasterEpoch(
                                epoch
                        );

                        ReplicationMetadata.updateReplicaState(
                                replId,
                                -1
                        );

                        KeyVersionManager.flushAll(
                                new HashSet<>(
                                        getAllKeys()
                                )
                        );

                        clearAllDataInternal();

                        AofManager.clear();

                        AofManager.appendCommand(
                                "FLUSHALL"
                        );

                        AofManager.appendCommand(
                                "REPLSEQ -1"
                        );
                    }

                    RespProtocol.writeSimpleString(
                            output,
                            "OK"
                    );

                    System.out.println(
                            "FULL SYNC started."
                    );

                    System.out.println(
                            "REPL ID = "
                                    + ReplicationId.shortId(
                                    replId
                            )
                                    + "..."
                    );

                    System.out.println(
                            "Epoch = "
                                    + epoch
                    );

                    System.out.println(
                            "Target sequence = "
                                    + sequence
                    );

                    continue;
                }

                // =================================================
                // FULL SYNC END
                //
                // REPLFULLSYNCEND replId epoch sequence
                // =================================================

                if (
                        operation.equals(
                                "REPLFULLSYNCEND"
                        )
                ) {

                    if (
                            !serverRole.isReplica()
                                    ||
                            !replicationConnection
                    ) {

                        RespProtocol.writeError(
                                output,
                                "Replication connection required"
                        );

                        continue;
                    }

                    String[] p =
                            command.split("\\s+");

                    if (p.length < 4) {

                        RespProtocol.writeError(
                                output,
                                "REPLFULLSYNCEND requires replId epoch sequence"
                        );

                        continue;
                    }

                    String replId =
                            p[1];

                    long epoch;
                    long sequence;

                    try {

                        epoch =
                                Long.parseLong(
                                        p[2]
                                );

                        sequence =
                                Long.parseLong(
                                        p[3]
                                );

                    } catch (
                            NumberFormatException e
                    ) {

                        RespProtocol.writeError(
                                output,
                                "Invalid full sync metadata"
                        );

                        continue;
                    }

                    synchronized (COMMAND_LOCK) {

                        if (!replicaFullSyncInProgress) {

                            RespProtocol.writeError(
                                    output,
                                    "No full sync in progress"
                            );

                            continue;
                        }

                        if (
                                !ReplicationId.matches(
                                        replId,
                                        replicaFullSyncReplicationId
                                )
                        ) {

                            RespProtocol.writeError(
                                    output,
                                    "Full sync replication ID mismatch"
                            );

                            continue;
                        }

                        if (
                                epoch
                                        !=
                                replicaFullSyncEpoch
                        ) {

                            RespProtocol.writeError(
                                    output,
                                    "Full sync epoch mismatch"
                            );

                            continue;
                        }

                        if (
                                sequence
                                        !=
                                replicaFullSyncTargetSequence
                        ) {

                            RespProtocol.writeError(
                                    output,
                                    "Full sync sequence mismatch"
                            );

                            continue;
                        }

                        replicaAppliedSequence.set(
                                sequence
                        );

                        ReplicationMetadata.setMasterEpoch(
                                epoch
                        );

                        ReplicationMetadata.updateReplicaState(
                                replId,
                                sequence
                        );

                        if (!checkpoint()) {

                            replicaAppliedSequence.set(
                                    -1
                            );

                            ReplicationMetadata.setReplicaSequence(
                                    -1
                            );

                            RespProtocol.writeError(
                                    output,
                                    "Could not save replica checkpoint"
                            );

                            continue;
                        }

                        replicaFullSyncInProgress =
                                false;

                        replicaFullSyncReplicationId =
                                null;

                        replicaFullSyncEpoch =
                                0;

                        replicaFullSyncTargetSequence =
                                0;
                    }

                    RespProtocol.writeSimpleString(
                            output,
                            "OK"
                    );

                    System.out.println(
                            "FULL SYNC completed."
                    );

                    System.out.println(
                            "Replica sequence = "
                                    + sequence
                    );

                    continue;
                }

                // =================================================
                // PARTIAL SYNC BEGIN
                //
                // REPLPARTIALSYNCBEGIN
                // replId epoch start end
                // =================================================

                if (
                        operation.equals(
                                "REPLPARTIALSYNCBEGIN"
                        )
                ) {

                    if (
                            !serverRole.isReplica()
                                    ||
                            !replicationConnection
                    ) {

                        RespProtocol.writeError(
                                output,
                                "Replication connection required"
                        );

                        continue;
                    }

                    String[] p =
                            command.split("\\s+");

                    if (p.length < 5) {

                        RespProtocol.writeError(
                                output,
                                "REPLPARTIALSYNCBEGIN requires replId epoch start end"
                        );

                        continue;
                    }

                    String replId =
                            p[1];

                    long epoch;
                    long start;
                    long end;

                    try {

                        if (
                                !ReplicationId.isValid(
                                        replId
                                )
                        ) {

                            throw new IllegalArgumentException();
                        }

                        epoch =
                                Long.parseLong(
                                        p[2]
                                );

                        start =
                                Long.parseLong(
                                        p[3]
                                );

                        end =
                                Long.parseLong(
                                        p[4]
                                );

                    } catch (Exception e) {

                        RespProtocol.writeError(
                                output,
                                "Invalid partial sync metadata"
                        );

                        continue;
                    }

                    synchronized (COMMAND_LOCK) {

                        if (replicaFullSyncInProgress) {

                            RespProtocol.writeError(
                                    output,
                                    "Full sync already in progress"
                            );

                            continue;
                        }

                        /*
                         * Most important Stage 23 protection:
                         * Partial sync is allowed only for the
                         * same replication history.
                         */
                        if (
                                !ReplicationMetadata
                                        .matchesUpstream(
                                                replId
                                        )
                        ) {

                            RespProtocol.writeError(
                                    output,
                                    "Replication ID mismatch; full sync required"
                            );

                            continue;
                        }

                        if (
                                ReplicationMetadata
                                        .getMasterEpoch()
                                        !=
                                epoch
                        ) {

                            RespProtocol.writeError(
                                    output,
                                    "Replication epoch mismatch; full sync required"
                            );

                            continue;
                        }

                        long current =
                                replicaAppliedSequence.get();

                        if (current < 0) {

                            RespProtocol.writeError(
                                    output,
                                    "Replica requires full synchronization"
                            );

                            continue;
                        }

                        if (
                                start
                                        !=
                                current + 1
                        ) {

                            RespProtocol.writeError(
                                    output,
                                    "Partial sync start sequence mismatch"
                            );

                            continue;
                        }

                        if (end < start) {

                            RespProtocol.writeError(
                                    output,
                                    "Invalid partial sync range"
                            );

                            continue;
                        }

                        replicaPartialSyncInProgress =
                                true;

                        replicaPartialSyncReplicationId =
                                ReplicationId.normalize(
                                        replId
                                );

                        replicaPartialSyncEpoch =
                                epoch;

                        replicaPartialSyncStartSequence =
                                start;

                        replicaPartialSyncEndSequence =
                                end;
                    }

                    RespProtocol.writeSimpleString(
                            output,
                            "OK"
                    );

                    System.out.println(
                            "PARTIAL SYNC started."
                    );

                    System.out.println(
                            "REPL ID = "
                                    + ReplicationId.shortId(
                                    replId
                            )
                                    + "..."
                    );

                    System.out.println(
                            "Epoch = "
                                    + epoch
                    );

                    System.out.println(
                            "Sequences "
                                    + start
                                    + " -> "
                                    + end
                    );

                    continue;
                }

                // =================================================
                // REPLCMD
                // =================================================

                if (
                        operation.equals(
                                "REPLCMD"
                        )
                ) {

                    if (
                            !serverRole.isReplica()
                                    ||
                            !replicationConnection
                    ) {

                        RespProtocol.writeError(
                                output,
                                "Replication connection required"
                        );

                        continue;
                    }

                    String[] p =
                            command.split(
                                    "\\s+",
                                    3
                            );

                    if (p.length < 3) {

                        RespProtocol.writeError(
                                output,
                                "REPLCMD requires sequence and command"
                        );

                        continue;
                    }

                    long sequence;

                    try {

                        sequence =
                                Long.parseLong(
                                        p[1]
                                );

                    } catch (
                            NumberFormatException e
                    ) {

                        RespProtocol.writeError(
                                output,
                                "Invalid replication sequence"
                        );

                        continue;
                    }

                    String actualCommand =
                            p[2];

                    synchronized (COMMAND_LOCK) {

                        if (replicaFullSyncInProgress) {

                            RespProtocol.writeError(
                                    output,
                                    "Full synchronization in progress"
                            );

                            continue;
                        }

                        long current =
                                replicaAppliedSequence.get();

                        if (current < 0) {

                            RespProtocol.writeError(
                                    output,
                                    "Replica requires full synchronization"
                            );

                            continue;
                        }

                        /*
                         * Duplicate replication command.
                         */
                        if (sequence <= current) {

                            RespProtocol.writeSimpleString(
                                    output,
                                    "OK"
                            );

                            continue;
                        }

                        /*
                         * Prevent gaps.
                         */
                        if (
                                sequence
                                        !=
                                current + 1
                        ) {

                            RespProtocol.writeError(
                                    output,
                                    "Replication sequence gap"
                            );

                            continue;
                        }

                        if (
                                replicaPartialSyncInProgress
                                        &&
                                (
                                        sequence
                                                <
                                        replicaPartialSyncStartSequence
                                                ||
                                        sequence
                                                >
                                        replicaPartialSyncEndSequence
                                )
                        ) {

                            RespProtocol.writeError(
                                    output,
                                    "Sequence outside partial sync range"
                            );

                            continue;
                        }

                        String response =
                                processCommand(
                                        actualCommand
                                );

                        if (
                                response.startsWith(
                                        "ERROR"
                                )
                        ) {

                            RespProtocol.writeError(
                                    output,
                                    response.replaceFirst(
                                            "^ERROR:\\s*",
                                            ""
                                    )
                            );

                            continue;
                        }

                        replicaAppliedSequence.set(
                                sequence
                        );

                        /*
                         * Persist replica offset in both AOF
                         * and metadata.
                         */
                        AofManager.appendCommand(
                                "REPLSEQ "
                                        + sequence
                        );

                        ReplicationMetadata.setReplicaSequence(
                                sequence
                        );
                    }

                    RespProtocol.writeSimpleString(
                            output,
                            "OK"
                    );

                    continue;
                }

                // =================================================
                // PARTIAL SYNC END
                //
                // REPLPARTIALSYNCEND replId epoch end
                // =================================================

                if (
                        operation.equals(
                                "REPLPARTIALSYNCEND"
                        )
                ) {

                    if (
                            !serverRole.isReplica()
                                    ||
                            !replicationConnection
                    ) {

                        RespProtocol.writeError(
                                output,
                                "Replication connection required"
                        );

                        continue;
                    }

                    String[] p =
                            command.split("\\s+");

                    if (p.length < 4) {

                        RespProtocol.writeError(
                                output,
                                "REPLPARTIALSYNCEND requires replId epoch sequence"
                        );

                        continue;
                    }

                    String replId =
                            p[1];

                    long epoch;
                    long end;

                    try {

                        epoch =
                                Long.parseLong(
                                        p[2]
                                );

                        end =
                                Long.parseLong(
                                        p[3]
                                );

                    } catch (
                            NumberFormatException e
                    ) {

                        RespProtocol.writeError(
                                output,
                                "Invalid partial sync metadata"
                        );

                        continue;
                    }

                    synchronized (COMMAND_LOCK) {

                        if (!replicaPartialSyncInProgress) {

                            RespProtocol.writeError(
                                    output,
                                    "No partial sync in progress"
                            );

                            continue;
                        }

                        if (
                                !ReplicationId.matches(
                                        replId,
                                        replicaPartialSyncReplicationId
                                )
                        ) {

                            RespProtocol.writeError(
                                    output,
                                    "Partial sync replication ID mismatch"
                            );

                            continue;
                        }

                        if (
                                epoch
                                        !=
                                replicaPartialSyncEpoch
                        ) {

                            RespProtocol.writeError(
                                    output,
                                    "Partial sync epoch mismatch"
                            );

                            continue;
                        }

                        if (
                                end
                                        !=
                                replicaPartialSyncEndSequence
                        ) {

                            RespProtocol.writeError(
                                    output,
                                    "Partial sync end sequence mismatch"
                            );

                            continue;
                        }

                        if (
                                replicaAppliedSequence.get()
                                        !=
                                end
                        ) {

                            RespProtocol.writeError(
                                    output,
                                    "Partial sync incomplete"
                            );

                            continue;
                        }

                        ReplicationMetadata.setMasterEpoch(
                                epoch
                        );

                        ReplicationMetadata.updateReplicaState(
                                replId,
                                end
                        );

                        if (!checkpoint()) {

                            RespProtocol.writeError(
                                    output,
                                    "Could not save partial sync checkpoint"
                            );

                            continue;
                        }

                        replicaPartialSyncInProgress =
                                false;

                        replicaPartialSyncReplicationId =
                                null;

                        replicaPartialSyncEpoch =
                                0;

                        replicaPartialSyncStartSequence =
                                0;

                        replicaPartialSyncEndSequence =
                                0;
                    }

                    RespProtocol.writeSimpleString(
                            output,
                            "OK"
                    );

                    System.out.println(
                            "PARTIAL SYNC completed."
                    );

                    System.out.println(
                            "Replica sequence = "
                                    + end
                    );

                    continue;
                }

                // =================================================
                // NORMAL CLIENT BLOCK DURING SYNC
                // =================================================

                if (
                        serverRole.isReplica()
                                &&
                        !replicationConnection
                                &&
                        (
                                replicaFullSyncInProgress
                                        ||
                                replicaPartialSyncInProgress
                        )
                ) {

                    RespProtocol.writeError(
                            output,
                            "TRYAGAIN Replica synchronization in progress"
                    );

                    continue;
                }

                // =================================================
                // WATCH
                // =================================================

                if (operation.equals("WATCH")) {

                    if (inTransaction) {

                        RespProtocol.writeError(
                                output,
                                "WATCH inside MULTI is not allowed"
                        );

                        continue;
                    }

                    String[] p =
                            command.split("\\s+");

                    if (p.length < 2) {

                        RespProtocol.writeError(
                                output,
                                "WATCH requires key"
                        );

                        continue;
                    }

                    synchronized (COMMAND_LOCK) {

                        for (
                                int i = 1;
                                i < p.length;
                                i++
                        ) {

                            isExpired(
                                    p[i]
                            );

                            KeyVersionManager.watch(
                                    watchedKeys,
                                    p[i]
                            );
                        }
                    }

                    RespProtocol.writeSimpleString(
                            output,
                            "OK"
                    );

                    continue;
                }

                // =================================================
                // UNWATCH
                // =================================================

                if (
                        operation.equals(
                                "UNWATCH"
                        )
                ) {

                    KeyVersionManager.clear(
                            watchedKeys
                    );

                    RespProtocol.writeSimpleString(
                            output,
                            "OK"
                    );

                    continue;
                }

                // =================================================
                // SUBSCRIBE
                // =================================================

                if (
                        operation.equals(
                                "SUBSCRIBE"
                        )
                ) {

                    if (inTransaction) {

                        RespProtocol.writeError(
                                output,
                                "SUBSCRIBE not allowed inside MULTI"
                        );

                        continue;
                    }

                    String[] p =
                            command.split("\\s+");

                    if (p.length < 2) {

                        RespProtocol.writeError(
                                output,
                                "SUBSCRIBE requires channel"
                        );

                        continue;
                    }

                    for (
                            int i = 1;
                            i < p.length;
                            i++
                    ) {

                        PubSubManager.subscribe(
                                subscriber,
                                p[i]
                        );
                    }

                    continue;
                }

                // =================================================
                // UNSUBSCRIBE
                // =================================================

                if (
                        operation.equals(
                                "UNSUBSCRIBE"
                        )
                ) {

                    String[] p =
                            command.split("\\s+");

                    if (p.length < 2) {

                        PubSubManager.unsubscribeAll(
                                subscriber
                        );

                    } else {

                        for (
                                int i = 1;
                                i < p.length;
                                i++
                        ) {

                            PubSubManager.unsubscribe(
                                    subscriber,
                                    p[i]
                            );
                        }
                    }

                    continue;
                }

                // =================================================
                // MULTI
                // =================================================

                if (
                        operation.equals(
                                "MULTI"
                        )
                ) {

                    if (inTransaction) {

                        RespProtocol.writeError(
                                output,
                                "MULTI calls can not be nested"
                        );

                        continue;
                    }

                    inTransaction =
                            true;

                    transactionQueue.clear();

                    RespProtocol.writeSimpleString(
                            output,
                            "OK"
                    );

                    continue;
                }

                // =================================================
                // DISCARD
                // =================================================

                if (
                        operation.equals(
                                "DISCARD"
                        )
                ) {

                    if (!inTransaction) {

                        RespProtocol.writeError(
                                output,
                                "DISCARD without MULTI"
                        );

                        continue;
                    }

                    inTransaction =
                            false;

                    transactionQueue.clear();

                    KeyVersionManager.clear(
                            watchedKeys
                    );

                    RespProtocol.writeSimpleString(
                            output,
                            "OK"
                    );

                    continue;
                }

                // =================================================
                // EXEC
                // =================================================

                if (
                        operation.equals(
                                "EXEC"
                        )
                ) {

                    if (!inTransaction) {

                        RespProtocol.writeError(
                                output,
                                "EXEC without MULTI"
                        );

                        continue;
                    }

                    boolean aborted =
                            false;

                    List<String> results =
                            new ArrayList<>();

                    synchronized (COMMAND_LOCK) {

                        for (
                                String key :
                                watchedKeys.keySet()
                        ) {

                            isExpired(
                                    key
                            );
                        }

                        if (
                                KeyVersionManager
                                        .hasChanged(
                                                watchedKeys
                                        )
                        ) {

                            aborted =
                                    true;

                        } else {

                            for (
                                    String queued :
                                    transactionQueue
                            ) {

                                results.add(
                                        processCommand(
                                                queued
                                        )
                                );
                            }
                        }
                    }

                    inTransaction =
                            false;

                    transactionQueue.clear();

                    KeyVersionManager.clear(
                            watchedKeys
                    );

                    if (aborted) {

                        writeNullArray(
                                output
                        );

                    } else {

                        RespProtocol.writeArray(
                                output,
                                results
                        );
                    }

                    continue;
                }

                // =================================================
                // QUEUE TRANSACTION COMMAND
                // =================================================

                if (inTransaction) {

                    if (
                            serverRole.isReplica()
                                    &&
                            !replicationConnection
                                    &&
                            isWriteCommand(
                                    command
                            )
                    ) {

                        RespProtocol.writeError(
                                output,
                                "READONLY Replica does not accept writes"
                        );

                        continue;
                    }

                    transactionQueue.add(
                            command
                    );

                    RespProtocol.writeSimpleString(
                            output,
                            "QUEUED"
                    );

                    continue;
                }

                // =================================================
                // REPLICA READONLY
                // =================================================

                if (
                        serverRole.isReplica()
                                &&
                        !replicationConnection
                                &&
                        isWriteCommand(
                                command
                        )
                ) {

                    RespProtocol.writeError(
                            output,
                            "READONLY Replica does not accept writes"
                    );

                    continue;
                }

                String response;

                synchronized (COMMAND_LOCK) {

                    response =
                            processCommand(
                                    command
                            );
                }

                sendResponse(
                        command,
                        response,
                        output
                );
            }

        } catch (IOException e) {

            System.out.println(
                    "Client disconnected."
            );

        } finally {

            KeyVersionManager.clear(
                    watchedKeys
            );

            if (subscriber != null) {

                PubSubManager.removeSubscriber(
                        subscriber
                );
            }

            try {

                clientSocket.close();

            } catch (
                    IOException ignored
            ) {
            }
        }
    }

    // =========================================================
    // PROMOTE REPLICA -> MASTER
    // =========================================================

    private static String promoteToMaster() {

        if (serverRole.isMaster()) {

            return "ERROR: Node is already MASTER";
        }

        if (
                replicaFullSyncInProgress
                        ||
                replicaPartialSyncInProgress
        ) {

            return "ERROR: Cannot promote while synchronization is in progress";
        }

        long currentSequence =
                replicaAppliedSequence.get();

        if (currentSequence < 0) {

            return "ERROR: Replica state is unsafe; full synchronization required";
        }

        System.out.println();
        System.out.println(
                "========================================"
        );

        System.out.println(
                "PROMOTING REPLICA TO MASTER"
        );

        System.out.println(
                "Port = "
                        + PORT
        );

        System.out.println(
                "Current sequence = "
                        + currentSequence
        );

        System.out.println(
                "Old upstream REPL ID = "
                        +
                        (
                                ReplicationMetadata
                                        .getUpstreamReplicationId()
                                        ==
                                null
                                        ?
                                "none"
                                        :
                                ReplicationMetadata
                                        .getUpstreamReplicationId()
                        )
        );

        System.out.println(
                "========================================"
        );

        if (!checkpoint()) {

            return "ERROR: Could not create promotion checkpoint";
        }

        /*
         * Preserve current replicated offset.
         */
        ReplicationMetadata.setReplicaSequence(
                currentSequence
        );

        /*
         * Very important:
         * promotion creates a NEW replication history.
         */
        ReplicationMetadata.rotateMasterIdentity();

        serverRole =
                NodeRole.MASTER;

        configurePromotionReplicas();

        startMasterReplication();

        System.out.println(
                "PROMOTION SUCCESSFUL"
        );

        System.out.println(
                "New Master = localhost:"
                        + PORT
        );

        System.out.println(
                "New REPL ID = "
                        +
                        ReplicationMetadata
                                .getMasterReplicationId()
        );

        System.out.println(
                "New Epoch = "
                        +
                        ReplicationMetadata
                                .getMasterEpoch()
        );

        System.out.println(
                "Starting Sequence = "
                        + currentSequence
        );

        System.out.println(
                "Configured downstream replicas = "
                        + configuredReplicas.size()
        );

        System.out.println(
                "========================================"
        );

        return "OK PROMOTED TO MASTER";
    }

    // =========================================================
    // CREATE FULL SYNC PAYLOAD
    // =========================================================

    private static FullSyncPayload createFullSyncPayload() {

        synchronized (COMMAND_LOCK) {

            cleanExpiredKeys();

            long sequence =
                    ReplicationManager
                            .getCurrentSequence();

            List<String> commands =
                    new ArrayList<>();

            commands.add(
                    "CONFIG SET MAX_KEYS "
                            + maxKeys
            );

            // =================================================
            // STRINGS
            // =================================================

            List<String> stringKeys =
                    new ArrayList<>(
                            database.keySet()
                    );

            Collections.sort(
                    stringKeys
            );

            for (String key : stringKeys) {

                commands.add(
                        "SET "
                                + key
                                + " "
                                + database.get(key)
                );
            }

            // =================================================
            // LISTS
            // =================================================

            List<String> listKeys =
                    new ArrayList<>(
                            lists.keySet()
                    );

            Collections.sort(
                    listKeys
            );

            for (String key : listKeys) {

                ConcurrentLinkedDeque<String> list =
                        lists.get(key);

                if (list == null) {
                    continue;
                }

                for (String value : list) {

                    commands.add(
                            "RPUSH "
                                    + key
                                    + " "
                                    + value
                    );
                }
            }

            // =================================================
            // SETS
            // =================================================

            List<String> setKeys =
                    new ArrayList<>(
                            sets.keySet()
                    );

            Collections.sort(
                    setKeys
            );

            for (String key : setKeys) {

                Set<String> set =
                        sets.get(key);

                if (set == null) {
                    continue;
                }

                List<String> members =
                        new ArrayList<>(
                                set
                        );

                Collections.sort(
                        members
                );

                for (String member : members) {

                    commands.add(
                            "SADD "
                                    + key
                                    + " "
                                    + member
                    );
                }
            }

            // =================================================
            // HASHES
            // =================================================

            List<String> hashKeys =
                    new ArrayList<>(
                            hashes.keySet()
                    );

            Collections.sort(
                    hashKeys
            );

            for (String key : hashKeys) {

                ConcurrentHashMap<String, String> hash =
                        hashes.get(key);

                if (hash == null) {
                    continue;
                }

                List<String> fields =
                        new ArrayList<>(
                                hash.keySet()
                        );

                Collections.sort(
                        fields
                );

                for (String field : fields) {

                    commands.add(
                            "HSET "
                                    + key
                                    + " "
                                    + field
                                    + " "
                                    + hash.get(field)
                    );
                }
            }

            // =================================================
            // SORTED SETS
            // =================================================

            List<String> zsetKeys =
                    new ArrayList<>(
                            sortedSets.keySet()
                    );

            Collections.sort(
                    zsetKeys
            );

            for (String key : zsetKeys) {

                SortedSetStore zset =
                        sortedSets.get(key);

                if (zset == null) {
                    continue;
                }

                Map<String, Double> members =
                        zset.toMap();

                List<String> names =
                        new ArrayList<>(
                                members.keySet()
                        );

                Collections.sort(
                        names
                );

                for (String member : names) {

                    commands.add(
                            "ZADD "
                                    + key
                                    + " "
                                    +
                                    SortedSetStore.formatScore(
                                            members.get(member)
                                    )
                                    + " "
                                    + member
                    );
                }
            }

            // =================================================
            // EXPIRATIONS LAST
            // =================================================

            long now =
                    System.currentTimeMillis();

            List<String> expiryKeys =
                    new ArrayList<>(
                            expirations.keySet()
                    );

            Collections.sort(
                    expiryKeys
            );

            for (String key : expiryKeys) {

                Long expiry =
                        expirations.get(key);

                if (
                        expiry != null
                                &&
                        expiry > now
                                &&
                        keyExists(key)
                ) {

                    commands.add(
                            "PEXPIREAT "
                                    + key
                                    + " "
                                    + expiry
                    );
                }
            }

            System.out.println(
                    "Created FULL SYNC payload."
            );

            System.out.println(
                    "REPL ID = "
                            +
                            ReplicationId.shortId(
                                    ReplicationMetadata
                                            .getMasterReplicationId()
                            )
                            + "..."
            );

            System.out.println(
                    "Epoch = "
                            +
                            ReplicationMetadata
                                    .getMasterEpoch()
            );

            System.out.println(
                    "Sequence = "
                            + sequence
            );

            System.out.println(
                    "Commands = "
                            + commands.size()
            );

            return new FullSyncPayload(
                    sequence,
                    commands
            );
        }
    }

    // =========================================================
    // WRITE COMMAND CHECK
    // =========================================================

    private static boolean isWriteCommand(
            String command
    ) {

        String operation =
                getOperation(
                        command
                );

        switch (operation) {

            case "SET":
            case "SETEX":
            case "DEL":
            case "INCR":
            case "DECR":
            case "EXPIRE":
            case "PEXPIREAT":
            case "PERSIST":
            case "LPUSH":
            case "RPUSH":
            case "LPOP":
            case "RPOP":
            case "SADD":
            case "SREM":
            case "HSET":
            case "HDEL":
            case "ZADD":
            case "ZREM":
            case "FLUSHALL":
                return true;

            case "CONFIG":

                String[] p =
                        command.split("\\s+");

                return p.length >= 2
                        &&
                        p[1].equalsIgnoreCase(
                                "SET"
                        );

            default:
                return false;
        }
    }

    // =========================================================
    // RECORD AOF + REPLICATION
    // =========================================================

    private static void recordAof(
            String command
    ) {

        if (replayingAof) {
            return;
        }

        AofManager.appendCommand(
                command
        );

        if (serverRole.isMaster()) {

            ReplicationManager.propagate(
                    command
            );
        }
    }

    // =========================================================
    // COMMAND PROCESSOR
    // =========================================================

    private static String processCommand(
            String command
    ) {

        command =
                command.trim();

        if (command.isEmpty()) {

            return "ERROR: Empty command";
        }

        String[] parts =
                command.split("\\s+");

        String operation =
                parts[0].toUpperCase();

        switch (operation) {

            // =================================================
            // FAILOVER STATUS
            // =================================================

            case "FAILOVERSTATUS":

                if (!serverRole.isMaster()) {

                    return
                            "role:"
                                    + serverRole
                                    +
                                    "\nport:"
                                    + PORT
                                    +
                                    "\nupstream_repl_id:"
                                    +
                                    (
                                            ReplicationMetadata
                                                    .getUpstreamReplicationId()
                                                    ==
                                            null
                                                    ?
                                            "none"
                                                    :
                                            ReplicationMetadata
                                                    .getUpstreamReplicationId()
                                    )
                                    +
                                    "\nepoch:"
                                    +
                                    ReplicationMetadata
                                            .getMasterEpoch()
                                    +
                                    "\nreplica_sequence:"
                                    + replicaAppliedSequence.get();
                }

                return
                        "role:"
                                + serverRole
                                +
                                "\nport:"
                                + PORT
                                +
                                "\n"
                                +
                                FailoverManager
                                        .getFailoverStatus();

            // =================================================
            // FAILOVER CANDIDATE
            // =================================================

            case "FAILOVERCANDIDATE":

                if (!serverRole.isMaster()) {

                    return "ERROR: FAILOVERCANDIDATE is available on master only";
                }

                FailoverManager.FailoverDecision decision =
                        FailoverManager
                                .selectBestCandidate();

                if (!decision.isPossible()) {

                    return "ERROR: "
                            + decision.getReason();
                }

                return decision.getCandidateId();

            // =================================================
            // PROMOTE
            // =================================================

            case "PROMOTE":

                return promoteToMaster();

            // =================================================
            // REPLICATION METADATA
            // =================================================

            case "REPLMETA":

                return
                        "role:"
                                + serverRole
                                +
                                "\nport:"
                                + PORT
                                +
                                "\n"
                                +
                                ReplicationMetadata
                                        .getStatus();

            // =================================================
            // PING
            // =================================================

            case "PING":

                return "PONG";

            // =================================================
            // TYPE
            // =================================================

            case "TYPE": {

                if (parts.length < 2) {

                    return "ERROR: TYPE requires key";
                }

                String key =
                        parts[1];

                if (isExpired(key)) {
                    return "none";
                }

                switch (getKeyType(key)) {

                    case STRING:
                        return "string";

                    case LIST:
                        return "list";

                    case SET:
                        return "set";

                    case HASH:
                        return "hash";

                    case ZSET:
                        return "zset";

                    default:
                        return "none";
                }
            }

            // =================================================
            // SET
            // =================================================

            case "SET": {

                String[] p =
                        command.split(
                                "\\s+",
                                3
                        );

                if (p.length < 3) {

                    return "ERROR: SET requires key and value";
                }

                String key =
                        p[1];

                String value =
                        p[2];

                ensureCapacityForNewKey(
                        key
                );

                removeKeyInternal(
                        key
                );

                database.put(
                        key,
                        value
                );

                touchKey(
                        key
                );

                KeyVersionManager.touch(
                        key
                );

                recordAof(
                        "SET "
                                + key
                                + " "
                                + value
                );

                return "OK";
            }

            // =================================================
            // SETEX
            // =================================================

            case "SETEX": {

                String[] p =
                        command.split(
                                "\\s+",
                                4
                        );

                if (p.length < 4) {

                    return "ERROR: SETEX requires key seconds value";
                }

                long seconds;

                try {

                    seconds =
                            Long.parseLong(
                                    p[2]
                            );

                } catch (
                        NumberFormatException e
                ) {

                    return "ERROR: seconds must be a number";
                }

                if (seconds <= 0) {

                    return "ERROR: seconds must be greater than 0";
                }

                String key =
                        p[1];

                String value =
                        p[3];

                ensureCapacityForNewKey(
                        key
                );

                removeKeyInternal(
                        key
                );

                database.put(
                        key,
                        value
                );

                long expiry =
                        System.currentTimeMillis()
                                +
                        seconds * 1000;

                expirations.put(
                        key,
                        expiry
                );

                touchKey(
                        key
                );

                KeyVersionManager.touch(
                        key
                );

                recordAof(
                        "SET "
                                + key
                                + " "
                                + value
                );

                recordAof(
                        "PEXPIREAT "
                                + key
                                + " "
                                + expiry
                );

                return "OK";
            }

            // =================================================
            // GET
            // =================================================

            case "GET": {

                if (parts.length < 2) {

                    return "ERROR: GET requires key";
                }

                String key =
                        parts[1];

                if (isExpired(key)) {

                    return "(nil)";
                }

                if (
                        isWrongType(
                                key,
                                KeyType.STRING
                        )
                ) {

                    return WRONGTYPE_ERROR;
                }

                String value =
                        database.get(
                                key
                        );

                if (value == null) {

                    return "(nil)";
                }

                touchKey(
                        key
                );

                return value;
            }

            // =================================================
            // INCR / DECR
            // =================================================

            case "INCR":
            case "DECR": {

                if (parts.length < 2) {

                    return "ERROR: key required";
                }

                String key =
                        parts[1];

                isExpired(
                        key
                );

                if (
                        isWrongType(
                                key,
                                KeyType.STRING
                        )
                ) {

                    return WRONGTYPE_ERROR;
                }

                if (!keyExists(key)) {

                    ensureCapacityForNewKey(
                            key
                    );
                }

                try {

                    int value =
                            Integer.parseInt(
                                    database.getOrDefault(
                                            key,
                                            "0"
                                    )
                            );

                    if (
                            operation.equals(
                                    "INCR"
                            )
                    ) {

                        value++;

                    } else {

                        value--;
                    }

                    database.put(
                            key,
                            String.valueOf(
                                    value
                            )
                    );

                    touchKey(
                            key
                    );

                    KeyVersionManager.touch(
                            key
                    );

                    recordAof(
                            operation
                                    + " "
                                    + key
                    );

                    return String.valueOf(
                            value
                    );

                } catch (
                        NumberFormatException e
                ) {

                    return "ERROR: value is not an integer";
                }
            }

            // =================================================
            // DEL
            // =================================================

            case "DEL": {

                if (parts.length < 2) {

                    return "ERROR: DEL requires key";
                }

                String key =
                        parts[1];

                if (isExpired(key)) {

                    return "0";
                }

                boolean removed =
                        removeKeyInternal(
                                key
                        );

                if (removed) {

                    KeyVersionManager.touch(
                            key
                    );

                    recordAof(
                            "DEL "
                                    + key
                    );
                }

                return removed
                        ?
                        "1"
                        :
                        "0";
            }

            // =================================================
            // EXISTS
            // =================================================

            case "EXISTS": {

                if (parts.length < 2) {

                    return "ERROR: EXISTS requires key";
                }

                String key =
                        parts[1];

                if (isExpired(key)) {

                    return "0";
                }

                if (keyExists(key)) {

                    touchKey(
                            key
                    );

                    return "1";
                }

                return "0";
            }

            // =================================================
            // KEYS
            // =================================================

            case "KEYS": {

                cleanExpiredKeys();

                Set<String> keys =
                        getAllKeys();

                return keys.isEmpty()
                        ?
                        "(empty)"
                        :
                        String.join(
                                ", ",
                                keys
                        );
            }

            // =================================================
            // EXPIRE
            // =================================================

            case "EXPIRE": {

                if (parts.length < 3) {

                    return "ERROR: EXPIRE requires key seconds";
                }

                String key =
                        parts[1];

                if (
                        isExpired(key)
                                ||
                        !keyExists(key)
                ) {

                    return "0";
                }

                long seconds;

                try {

                    seconds =
                            Long.parseLong(
                                    parts[2]
                            );

                } catch (
                        NumberFormatException e
                ) {

                    return "ERROR: seconds must be a number";
                }

                if (seconds <= 0) {

                    removeKeyInternal(
                            key
                    );

                    KeyVersionManager.touch(
                            key
                    );

                    recordAof(
                            "DEL "
                                    + key
                    );

                    return "1";
                }

                long expiry =
                        System.currentTimeMillis()
                                +
                        seconds * 1000;

                expirations.put(
                        key,
                        expiry
                );

                touchKey(
                        key
                );

                KeyVersionManager.touch(
                        key
                );

                recordAof(
                        "PEXPIREAT "
                                + key
                                + " "
                                + expiry
                );

                return "1";
            }

            // =================================================
            // PEXPIREAT
            // =================================================

            case "PEXPIREAT": {

                if (parts.length < 3) {

                    return "ERROR: PEXPIREAT requires key timestamp";
                }

                String key =
                        parts[1];

                if (!keyExists(key)) {

                    return "0";
                }

                long timestamp;

                try {

                    timestamp =
                            Long.parseLong(
                                    parts[2]
                            );

                } catch (
                        NumberFormatException e
                ) {

                    return "ERROR: timestamp must be a number";
                }

                if (
                        timestamp
                                <=
                        System.currentTimeMillis()
                ) {

                    removeKeyInternal(
                            key
                    );

                    KeyVersionManager.touch(
                            key
                    );

                    recordAof(
                            "DEL "
                                    + key
                    );

                    return "1";
                }

                expirations.put(
                        key,
                        timestamp
                );

                touchKey(
                        key
                );

                KeyVersionManager.touch(
                        key
                );

                recordAof(
                        "PEXPIREAT "
                                + key
                                + " "
                                + timestamp
                );

                return "1";
            }

            // =================================================
            // TTL / PTTL
            // =================================================

            case "TTL":
            case "PTTL": {

                if (parts.length < 2) {

                    return "ERROR: key required";
                }

                String key =
                        parts[1];

                if (
                        isExpired(key)
                                ||
                        !keyExists(key)
                ) {

                    return "-2";
                }

                Long expiry =
                        expirations.get(
                                key
                        );

                if (expiry == null) {

                    return "-1";
                }

                touchKey(
                        key
                );

                long remaining =
                        expiry
                                -
                        System.currentTimeMillis();

                if (
                        operation.equals(
                                "TTL"
                        )
                ) {

                    remaining /=
                            1000;
                }

                return String.valueOf(
                        Math.max(
                                remaining,
                                0
                        )
                );
            }

            // =================================================
            // PERSIST
            // =================================================

            case "PERSIST": {

                if (parts.length < 2) {

                    return "ERROR: PERSIST requires key";
                }

                String key =
                        parts[1];

                if (
                        isExpired(key)
                                ||
                        !keyExists(key)
                ) {

                    return "0";
                }

                Long old =
                        expirations.remove(
                                key
                        );

                if (old == null) {

                    return "0";
                }

                touchKey(
                        key
                );

                KeyVersionManager.touch(
                        key
                );

                recordAof(
                        "PERSIST "
                                + key
                );

                return "1";
            }

            // =================================================
            // LIST PUSH
            // =================================================

            case "LPUSH":
            case "RPUSH": {

                String[] p =
                        command.split(
                                "\\s+",
                                3
                        );

                if (p.length < 3) {

                    return "ERROR: list command requires key value";
                }

                String key =
                        p[1];

                String value =
                        p[2];

                isExpired(
                        key
                );

                if (
                        isWrongType(
                                key,
                                KeyType.LIST
                        )
                ) {

                    return WRONGTYPE_ERROR;
                }

                if (!keyExists(key)) {

                    ensureCapacityForNewKey(
                            key
                    );
                }

                ConcurrentLinkedDeque<String> list =
                        lists.computeIfAbsent(
                                key,
                                k ->
                                        new ConcurrentLinkedDeque<>()
                        );

                if (
                        operation.equals(
                                "LPUSH"
                        )
                ) {

                    list.addFirst(
                            value
                    );

                } else {

                    list.addLast(
                            value
                    );
                }

                touchKey(
                        key
                );

                KeyVersionManager.touch(
                        key
                );

                recordAof(
                        operation
                                + " "
                                + key
                                + " "
                                + value
                );

                return String.valueOf(
                        list.size()
                );
            }

            // =================================================
            // LIST POP
            // =================================================

            case "LPOP":
            case "RPOP": {

                if (parts.length < 2) {

                    return "ERROR: list command requires key";
                }

                String key =
                        parts[1];

                if (isExpired(key)) {

                    return "(nil)";
                }

                if (
                        isWrongType(
                                key,
                                KeyType.LIST
                        )
                ) {

                    return WRONGTYPE_ERROR;
                }

                ConcurrentLinkedDeque<String> list =
                        lists.get(
                                key
                        );

                if (
                        list == null
                                ||
                        list.isEmpty()
                ) {

                    return "(nil)";
                }

                String value =
                        operation.equals(
                                "LPOP"
                        )
                                ?
                        list.pollFirst()
                                :
                        list.pollLast();

                if (list.isEmpty()) {

                    removeKeyInternal(
                            key
                    );

                } else {

                    touchKey(
                            key
                    );
                }

                KeyVersionManager.touch(
                        key
                );

                recordAof(
                        operation
                                + " "
                                + key
                );

                return value;
            }

            // =================================================
            // LRANGE
            // =================================================

            case "LRANGE": {

                if (parts.length < 2) {

                    return "ERROR: LRANGE requires key";
                }

                String key =
                        parts[1];

                if (isExpired(key)) {

                    return "(empty)";
                }

                if (
                        isWrongType(
                                key,
                                KeyType.LIST
                        )
                ) {

                    return WRONGTYPE_ERROR;
                }

                ConcurrentLinkedDeque<String> list =
                        lists.get(
                                key
                        );

                if (
                        list == null
                                ||
                        list.isEmpty()
                ) {

                    return "(empty)";
                }

                touchKey(
                        key
                );

                return String.join(
                        ", ",
                        list
                );
            }

            // =================================================
            // SET ADD / REMOVE
            // =================================================

            case "SADD":
            case "SREM": {

                if (parts.length < 3) {

                    return "ERROR: set command requires key member";
                }

                String key =
                        parts[1];

                String member =
                        parts[2];

                isExpired(
                        key
                );

                if (
                        isWrongType(
                                key,
                                KeyType.SET
                        )
                ) {

                    return WRONGTYPE_ERROR;
                }

                if (
                        operation.equals(
                                "SADD"
                        )
                ) {

                    if (!keyExists(key)) {

                        ensureCapacityForNewKey(
                                key
                        );
                    }

                    Set<String> set =
                            sets.computeIfAbsent(
                                    key,
                                    k ->
                                            ConcurrentHashMap
                                                    .newKeySet()
                            );

                    boolean added =
                            set.add(
                                    member
                            );

                    if (added) {

                        touchKey(
                                key
                        );

                        KeyVersionManager.touch(
                                key
                        );

                        recordAof(
                                "SADD "
                                        + key
                                        + " "
                                        + member
                        );
                    }

                    return added
                            ?
                            "1"
                            :
                            "0";
                }

                Set<String> set =
                        sets.get(
                                key
                        );

                if (set == null) {

                    return "0";
                }

                boolean removed =
                        set.remove(
                                member
                        );

                if (removed) {

                    KeyVersionManager.touch(
                            key
                    );

                    recordAof(
                            "SREM "
                                    + key
                                    + " "
                                    + member
                    );
                }

                if (set.isEmpty()) {

                    removeKeyInternal(
                            key
                    );

                } else {

                    touchKey(
                            key
                    );
                }

                return removed
                        ?
                        "1"
                        :
                        "0";
            }

            // =================================================
            // SISMEMBER
            // =================================================

            case "SISMEMBER": {

                if (parts.length < 3) {

                    return "ERROR: SISMEMBER requires key member";
                }

                String key =
                        parts[1];

                if (isExpired(key)) {

                    return "0";
                }

                if (
                        isWrongType(
                                key,
                                KeyType.SET
                        )
                ) {

                    return WRONGTYPE_ERROR;
                }

                Set<String> set =
                        sets.get(
                                key
                        );

                if (set == null) {

                    return "0";
                }

                touchKey(
                        key
                );

                return set.contains(
                        parts[2]
                )
                        ?
                        "1"
                        :
                        "0";
            }

            // =================================================
            // SMEMBERS
            // =================================================

            case "SMEMBERS": {

                if (parts.length < 2) {

                    return "ERROR: SMEMBERS requires key";
                }

                String key =
                        parts[1];

                if (isExpired(key)) {

                    return "(empty)";
                }

                if (
                        isWrongType(
                                key,
                                KeyType.SET
                        )
                ) {

                    return WRONGTYPE_ERROR;
                }

                Set<String> set =
                        sets.get(
                                key
                        );

                if (
                        set == null
                                ||
                        set.isEmpty()
                ) {

                    return "(empty)";
                }

                touchKey(
                        key
                );

                return String.join(
                        ", ",
                        set
                );
            }

            // =================================================
            // HSET
            // =================================================

            case "HSET": {

                String[] p =
                        command.split(
                                "\\s+",
                                4
                        );

                if (p.length < 4) {

                    return "ERROR: HSET requires key field value";
                }

                String key =
                        p[1];

                String field =
                        p[2];

                String value =
                        p[3];

                isExpired(
                        key
                );

                if (
                        isWrongType(
                                key,
                                KeyType.HASH
                        )
                ) {

                    return WRONGTYPE_ERROR;
                }

                if (!keyExists(key)) {

                    ensureCapacityForNewKey(
                            key
                    );
                }

                ConcurrentHashMap<String, String> hash =
                        hashes.computeIfAbsent(
                                key,
                                k ->
                                        new ConcurrentHashMap<>()
                        );

                String old =
                        hash.put(
                                field,
                                value
                        );

                touchKey(
                        key
                );

                KeyVersionManager.touch(
                        key
                );

                recordAof(
                        "HSET "
                                + key
                                + " "
                                + field
                                + " "
                                + value
                );

                return old == null
                        ?
                        "1"
                        :
                        "0";
            }

            // =================================================
            // HGET
            // =================================================

            case "HGET": {

                if (parts.length < 3) {

                    return "ERROR: HGET requires key field";
                }

                String key =
                        parts[1];

                if (isExpired(key)) {

                    return "(nil)";
                }

                if (
                        isWrongType(
                                key,
                                KeyType.HASH
                        )
                ) {

                    return WRONGTYPE_ERROR;
                }

                ConcurrentHashMap<String, String> hash =
                        hashes.get(
                                key
                        );

                if (hash == null) {

                    return "(nil)";
                }

                touchKey(
                        key
                );

                String value =
                        hash.get(
                                parts[2]
                        );

                return value == null
                        ?
                        "(nil)"
                        :
                        value;
            }

            // =================================================
            // HGETALL
            // =================================================

            case "HGETALL": {

                if (parts.length < 2) {

                    return "ERROR: HGETALL requires key";
                }

                String key =
                        parts[1];

                if (isExpired(key)) {

                    return "(empty)";
                }

                if (
                        isWrongType(
                                key,
                                KeyType.HASH
                        )
                ) {

                    return WRONGTYPE_ERROR;
                }

                ConcurrentHashMap<String, String> hash =
                        hashes.get(
                                key
                        );

                if (
                        hash == null
                                ||
                        hash.isEmpty()
                ) {

                    return "(empty)";
                }

                touchKey(
                        key
                );

                StringBuilder result =
                        new StringBuilder();

                for (
                        Map.Entry<String, String> entry :
                        hash.entrySet()
                ) {

                    if (result.length() > 0) {

                        result.append(
                                ", "
                        );
                    }

                    result.append(
                            entry.getKey()
                    );

                    result.append("=");

                    result.append(
                            entry.getValue()
                    );
                }

                return result.toString();
            }

            // =================================================
            // HDEL
            // =================================================

            case "HDEL": {

                if (parts.length < 3) {

                    return "ERROR: HDEL requires key field";
                }

                String key =
                        parts[1];

                if (isExpired(key)) {

                    return "0";
                }

                if (
                        isWrongType(
                                key,
                                KeyType.HASH
                        )
                ) {

                    return WRONGTYPE_ERROR;
                }

                ConcurrentHashMap<String, String> hash =
                        hashes.get(
                                key
                        );

                if (hash == null) {

                    return "0";
                }

                String removed =
                        hash.remove(
                                parts[2]
                        );

                if (removed != null) {

                    KeyVersionManager.touch(
                            key
                    );

                    recordAof(
                            "HDEL "
                                    + key
                                    + " "
                                    + parts[2]
                    );
                }

                if (hash.isEmpty()) {

                    removeKeyInternal(
                            key
                    );

                } else {

                    touchKey(
                            key
                    );
                }

                return removed != null
                        ?
                        "1"
                        :
                        "0";
            }

            // =================================================
            // HEXISTS
            // =================================================

            case "HEXISTS": {

                if (parts.length < 3) {

                    return "ERROR: HEXISTS requires key field";
                }

                String key =
                        parts[1];

                if (isExpired(key)) {

                    return "0";
                }

                if (
                        isWrongType(
                                key,
                                KeyType.HASH
                        )
                ) {

                    return WRONGTYPE_ERROR;
                }

                ConcurrentHashMap<String, String> hash =
                        hashes.get(
                                key
                        );

                if (hash == null) {

                    return "0";
                }

                touchKey(
                        key
                );

                return hash.containsKey(
                        parts[2]
                )
                        ?
                        "1"
                        :
                        "0";
            }

            // =================================================
            // ZADD
            // =================================================

            case "ZADD": {

                String[] p =
                        command.split(
                                "\\s+",
                                4
                        );

                if (p.length < 4) {

                    return "ERROR: ZADD requires key score member";
                }

                String key =
                        p[1];

                double score;

                try {

                    score =
                            Double.parseDouble(
                                    p[2]
                            );

                } catch (
                        NumberFormatException e
                ) {

                    return "ERROR: invalid score";
                }

                if (Double.isNaN(score)) {

                    return "ERROR: score cannot be NaN";
                }

                isExpired(
                        key
                );

                if (
                        isWrongType(
                                key,
                                KeyType.ZSET
                        )
                ) {

                    return WRONGTYPE_ERROR;
                }

                if (!keyExists(key)) {

                    ensureCapacityForNewKey(
                            key
                    );
                }

                SortedSetStore zset =
                        sortedSets.computeIfAbsent(
                                key,
                                k ->
                                        new SortedSetStore()
                        );

                int result =
                        zset.add(
                                p[3],
                                score
                        );

                touchKey(
                        key
                );

                KeyVersionManager.touch(
                        key
                );

                recordAof(
                        "ZADD "
                                + key
                                + " "
                                +
                                SortedSetStore.formatScore(
                                        score
                                )
                                + " "
                                + p[3]
                );

                return String.valueOf(
                        result
                );
            }

            // =================================================
            // ZRANGE
            // =================================================

            case "ZRANGE": {

                if (parts.length < 2) {

                    return "ERROR: ZRANGE requires key";
                }

                String key =
                        parts[1];

                if (isExpired(key)) {

                    return "(empty)";
                }

                if (
                        isWrongType(
                                key,
                                KeyType.ZSET
                        )
                ) {

                    return WRONGTYPE_ERROR;
                }

                SortedSetStore zset =
                        sortedSets.get(
                                key
                        );

                if (
                        zset == null
                                ||
                        zset.isEmpty()
                ) {

                    return "(empty)";
                }

                touchKey(
                        key
                );

                boolean withScores =
                        parts.length >= 3
                                &&
                        parts[2].equalsIgnoreCase(
                                "WITHSCORES"
                        );

                List<String> result =
                        withScores
                                ?
                        zset.rangeWithScores()
                                :
                        zset.range();

                return result.isEmpty()
                        ?
                        "(empty)"
                        :
                        String.join(
                                ", ",
                                result
                        );
            }

            // =================================================
            // ZSCORE
            // =================================================

            case "ZSCORE": {

                if (parts.length < 3) {

                    return "ERROR: ZSCORE requires key member";
                }

                String key =
                        parts[1];

                if (isExpired(key)) {

                    return "(nil)";
                }

                if (
                        isWrongType(
                                key,
                                KeyType.ZSET
                        )
                ) {

                    return WRONGTYPE_ERROR;
                }

                SortedSetStore zset =
                        sortedSets.get(
                                key
                        );

                if (zset == null) {

                    return "(nil)";
                }

                Double score =
                        zset.getScore(
                                parts[2]
                        );

                if (score == null) {

                    return "(nil)";
                }

                touchKey(
                        key
                );

                return SortedSetStore.formatScore(
                        score
                );
            }

            // =================================================
            // ZREM
            // =================================================

            case "ZREM": {

                if (parts.length < 3) {

                    return "ERROR: ZREM requires key member";
                }

                String key =
                        parts[1];

                if (isExpired(key)) {

                    return "0";
                }

                if (
                        isWrongType(
                                key,
                                KeyType.ZSET
                        )
                ) {

                    return WRONGTYPE_ERROR;
                }

                SortedSetStore zset =
                        sortedSets.get(
                                key
                        );

                if (zset == null) {

                    return "0";
                }

                int result =
                        zset.remove(
                                parts[2]
                        );

                if (result == 1) {

                    KeyVersionManager.touch(
                            key
                    );

                    recordAof(
                            "ZREM "
                                    + key
                                    + " "
                                    + parts[2]
                    );
                }

                if (zset.isEmpty()) {

                    removeKeyInternal(
                            key
                    );

                } else {

                    touchKey(
                            key
                    );
                }

                return String.valueOf(
                        result
                );
            }

            // =================================================
            // ZCARD
            // =================================================

            case "ZCARD": {

                if (parts.length < 2) {

                    return "ERROR: ZCARD requires key";
                }

                String key =
                        parts[1];

                if (isExpired(key)) {

                    return "0";
                }

                if (
                        isWrongType(
                                key,
                                KeyType.ZSET
                        )
                ) {

                    return WRONGTYPE_ERROR;
                }

                SortedSetStore zset =
                        sortedSets.get(
                                key
                        );

                if (zset == null) {

                    return "0";
                }

                touchKey(
                        key
                );

                return String.valueOf(
                        zset.size()
                );
            }

            // =================================================
            // PUBLISH
            // =================================================

            case "PUBLISH": {

                String[] p =
                        command.split(
                                "\\s+",
                                3
                        );

                if (p.length < 3) {

                    return "ERROR: PUBLISH requires channel message";
                }

                return String.valueOf(
                        PubSubManager.publish(
                                p[1],
                                p[2]
                        )
                );
            }

            // =================================================
            // DBSIZE
            // =================================================

            case "DBSIZE":

                cleanExpiredKeys();

                return String.valueOf(
                        getAllKeys().size()
                );

            // =================================================
            // REPLICATIONINFO
            // =================================================

            case "REPLICATIONINFO":

                if (serverRole.isMaster()) {

                    return
                            "role:"
                                    + serverRole
                                    +
                                    "\nport:"
                                    + PORT
                                    +
                                    "\n"
                                    +
                                    ReplicationManager
                                            .getStatus();
                }

                return
                        "role:"
                                + serverRole
                                +
                                "\nport:"
                                + PORT
                                +
                                "\nupstream_repl_id:"
                                +
                                (
                                        ReplicationMetadata
                                                .getUpstreamReplicationId()
                                                ==
                                        null
                                                ?
                                        "none"
                                                :
                                        ReplicationMetadata
                                                .getUpstreamReplicationId()
                                )
                                +
                                "\nupstream_epoch:"
                                +
                                ReplicationMetadata
                                        .getMasterEpoch()
                                +
                                "\nreplica_applied_sequence:"
                                + replicaAppliedSequence.get()
                                +
                                "\nfull_sync_in_progress:"
                                + replicaFullSyncInProgress
                                +
                                "\npartial_sync_in_progress:"
                                + replicaPartialSyncInProgress;

            // =================================================
            // AOF STATUS
            // =================================================

            case "AOFSTATUS":

                return
                        "aof_enabled:yes"
                                +
                                "\naof_file:"
                                + AofManager.getFileName()
                                +
                                "\naof_size_bytes:"
                                + AofManager.getFileSize()
                                +
                                "\naof_commands:"
                                + AofManager.getCommandCount();

            // =================================================
            // INFO
            // =================================================

            case "INFO":

                cleanExpiredKeys();

                return
                        "java_redis_version:2.4"
                                +
                                "\nrole:"
                                + serverRole
                                +
                                "\nport:"
                                + PORT
                                +
                                "\ntotal_keys:"
                                + getAllKeys().size()
                                +
                                "\nstring_keys:"
                                + database.size()
                                +
                                "\nlist_keys:"
                                + lists.size()
                                +
                                "\nset_keys:"
                                + sets.size()
                                +
                                "\nhash_keys:"
                                + hashes.size()
                                +
                                "\nzset_keys:"
                                + sortedSets.size()
                                +
                                "\nmax_keys:"
                                + maxKeys
                                +
                                "\nevicted_keys:"
                                + evictedKeys.get()
                                +
                                "\neviction_policy:allkeys-lru"
                                +
                                "\npersistence:snapshot+aof"
                                +
                                "\nauthentication:enabled"
                                +
                                "\ntransactions:enabled"
                                +
                                "\nwatch_transactions:enabled"
                                +
                                "\nreplication:enabled"
                                +
                                "\nfull_resync:enabled"
                                +
                                "\npartial_resync:enabled"
                                +
                                "\nreplication_backlog:enabled"
                                +
                                "\nmultiple_replicas:enabled"
                                +
                                "\nheartbeat_monitoring:enabled"
                                +
                                "\nmanual_failover:enabled"
                                +
                                "\nreplication_id_safety:enabled"
                                +
                                "\nmaster_repl_id:"
                                +
                                (
                                        serverRole.isMaster()
                                                ?
                                        ReplicationMetadata
                                                .getMasterReplicationId()
                                                :
                                        "none"
                                )
                                +
                                "\nupstream_repl_id:"
                                +
                                (
                                        serverRole.isReplica()
                                                &&
                                        ReplicationMetadata
                                                .getUpstreamReplicationId()
                                                !=
                                        null
                                                ?
                                        ReplicationMetadata
                                                .getUpstreamReplicationId()
                                                :
                                        "none"
                                )
                                +
                                "\nreplication_epoch:"
                                +
                                ReplicationMetadata
                                        .getMasterEpoch()
                                +
                                "\nreplica_applied_sequence:"
                                + replicaAppliedSequence.get()
                                +
                                "\nconfigured_replicas:"
                                +
                                (
                                        serverRole.isMaster()
                                                ?
                                        ReplicationManager
                                                .getReplicaCount()
                                                :
                                        0
                                )
                                +
                                "\nconnected_replicas:"
                                +
                                (
                                        serverRole.isMaster()
                                                ?
                                        ReplicationManager
                                                .getConnectedReplicaCount()
                                                :
                                        0
                                );

            // =================================================
            // CONFIG
            // =================================================

            case "CONFIG": {

                if (parts.length < 3) {

                    return "ERROR: CONFIG GET/SET MAX_KEYS";
                }

                if (
                        !parts[2].equalsIgnoreCase(
                                "MAX_KEYS"
                        )
                ) {

                    return "ERROR: Unsupported config";
                }

                if (
                        parts[1].equalsIgnoreCase(
                                "GET"
                        )
                ) {

                    return "MAX_KEYS, "
                            + maxKeys;
                }

                if (
                        parts[1].equalsIgnoreCase(
                                "SET"
                        )
                ) {

                    if (parts.length < 4) {

                        return "ERROR: value required";
                    }

                    int value;

                    try {

                        value =
                                Integer.parseInt(
                                        parts[3]
                                );

                    } catch (
                            NumberFormatException e
                    ) {

                        return "ERROR: invalid value";
                    }

                    if (value < 1) {

                        return "ERROR: MAX_KEYS must be >= 1";
                    }

                    maxKeys =
                            value;

                    cleanExpiredKeys();

                    while (
                            getAllKeys().size()
                                    >
                            maxKeys
                    ) {

                        evictLeastRecentlyUsed();
                    }

                    recordAof(
                            "CONFIG SET MAX_KEYS "
                                    + value
                    );

                    return "OK";
                }

                return "ERROR: CONFIG supports GET/SET";
            }

            // =================================================
            // SAVE
            // =================================================

            case "SAVE":

                return checkpoint()
                        ?
                        "OK"
                        :
                        "ERROR: Snapshot save failed";

            // =================================================
            // FLUSHALL
            // =================================================

            case "FLUSHALL": {

                Set<String> oldKeys =
                        new HashSet<>(
                                getAllKeys()
                        );

                KeyVersionManager.flushAll(
                        oldKeys
                );

                clearAllDataInternal();

                recordAof(
                        "FLUSHALL"
                );

                return "OK";
            }

            default:

                return "ERROR: Unknown command";
        }
    }

    // =========================================================
    // ACTIVE EXPIRATION
    // =========================================================

    private static void startExpirationCleaner() {

        expirationCleaner.scheduleAtFixedRate(
                () -> {

                    synchronized (COMMAND_LOCK) {

                        long now =
                                System.currentTimeMillis();

                        List<Map.Entry<String, Long>> entries =
                                new ArrayList<>(
                                        expirations.entrySet()
                                );

                        for (
                                Map.Entry<String, Long> entry :
                                entries
                        ) {

                            if (
                                    now
                                            >=
                                    entry.getValue()
                            ) {

                                String key =
                                        entry.getKey();

                                boolean removed =
                                        removeKeyInternal(
                                                key
                                        );

                                if (removed) {

                                    KeyVersionManager.touch(
                                            key
                                    );

                                    recordAof(
                                            "DEL "
                                                    + key
                                    );

                                    System.out.println(
                                            "Expired key removed: "
                                                    + key
                                    );
                                }
                            }
                        }
                    }
                },
                1,
                1,
                TimeUnit.SECONDS
        );
    }

    // =========================================================
    // AOF REPLAY
    // =========================================================

    private static void replayAof() {

        List<String> commands =
                AofManager.readCommands();

        if (commands.isEmpty()) {

            System.out.println(
                    "No AOF commands to replay."
            );

            return;
        }

        System.out.println(
                "Replaying "
                        + commands.size()
                        + " AOF command(s)..."
        );

        int successful =
                0;

        for (String command : commands) {

            try {

                if (
                        command.toUpperCase()
                                .startsWith(
                                        "REPLSEQ "
                                )
                ) {

                    String[] p =
                            command.split("\\s+");

                    if (p.length >= 2) {

                        replicaAppliedSequence.set(
                                Long.parseLong(
                                        p[1]
                                )
                        );

                        successful++;
                    }

                    continue;
                }

                String response =
                        processCommand(
                                command
                        );

                if (
                        !response.startsWith(
                                "ERROR"
                        )
                ) {

                    successful++;
                }

            } catch (Exception e) {

                System.out.println(
                        "AOF replay failed: "
                                + command
                );
            }
        }

        System.out.println(
                "AOF replay complete: "
                        + successful
                        + "/"
                        + commands.size()
        );
    }

    // =========================================================
    // COMMAND HELPER
    // =========================================================

    private static String getOperation(
            String command
    ) {

        return command
                .trim()
                .split("\\s+")[0]
                .toUpperCase();
    }

    // =========================================================
    // KEY TYPE
    // =========================================================

    private static KeyType getKeyType(
            String key
    ) {

        if (database.containsKey(key)) {
            return KeyType.STRING;
        }

        if (lists.containsKey(key)) {
            return KeyType.LIST;
        }

        if (sets.containsKey(key)) {
            return KeyType.SET;
        }

        if (hashes.containsKey(key)) {
            return KeyType.HASH;
        }

        if (sortedSets.containsKey(key)) {
            return KeyType.ZSET;
        }

        return KeyType.NONE;
    }

    private static boolean isWrongType(
            String key,
            KeyType expected
    ) {

        KeyType actual =
                getKeyType(
                        key
                );

        return actual != KeyType.NONE
                &&
                actual != expected;
    }

    private static boolean keyExists(
            String key
    ) {

        return getKeyType(
                key
        ) != KeyType.NONE;
    }

    // =========================================================
    // ACCESS TRACKING
    // =========================================================

    private static void touchKey(
            String key
    ) {

        if (!keyExists(key)) {
            return;
        }

        accessOrder.put(
                key,
                accessCounter.incrementAndGet()
        );
    }

    // =========================================================
    // DELETE KEY INTERNAL
    // =========================================================

    private static boolean removeKeyInternal(
            String key
    ) {

        boolean removed =
                false;

        if (database.remove(key) != null) {
            removed = true;
        }

        if (lists.remove(key) != null) {
            removed = true;
        }

        if (sets.remove(key) != null) {
            removed = true;
        }

        if (hashes.remove(key) != null) {
            removed = true;
        }

        if (sortedSets.remove(key) != null) {
            removed = true;
        }

        expirations.remove(
                key
        );

        accessOrder.remove(
                key
        );

        return removed;
    }

    // =========================================================
    // CLEAR ALL INTERNAL
    // =========================================================

    private static void clearAllDataInternal() {

        database.clear();

        expirations.clear();

        lists.clear();

        sets.clear();

        hashes.clear();

        sortedSets.clear();

        accessOrder.clear();
    }

    // =========================================================
    // GET ALL KEYS
    // =========================================================

    private static Set<String> getAllKeys() {

        Set<String> keys =
                ConcurrentHashMap
                        .newKeySet();

        keys.addAll(
                database.keySet()
        );

        keys.addAll(
                lists.keySet()
        );

        keys.addAll(
                sets.keySet()
        );

        keys.addAll(
                hashes.keySet()
        );

        keys.addAll(
                sortedSets.keySet()
        );

        return keys;
    }

    // =========================================================
    // EXPIRY
    // =========================================================

    private static boolean isExpired(
            String key
    ) {

        Long expiry =
                expirations.get(
                        key
                );

        if (expiry == null) {

            return false;
        }

        if (
                System.currentTimeMillis()
                        >=
                expiry
        ) {

            boolean removed =
                    removeKeyInternal(
                            key
                    );

            if (removed) {

                KeyVersionManager.touch(
                        key
                );

                recordAof(
                        "DEL "
                                + key
                );
            }

            return true;
        }

        return false;
    }

    private static void cleanExpiredKeys() {

        for (
                String key :
                new ArrayList<>(
                        getAllKeys()
                )
        ) {

            isExpired(
                    key
            );
        }
    }

    // =========================================================
    // LRU CAPACITY
    // =========================================================

    private static void ensureCapacityForNewKey(
            String key
    ) {

        cleanExpiredKeys();

        if (keyExists(key)) {
            return;
        }

        while (
                getAllKeys().size()
                        >=
                maxKeys
        ) {

            evictLeastRecentlyUsed();
        }
    }

    private static void evictLeastRecentlyUsed() {

        cleanExpiredKeys();

        Set<String> keys =
                getAllKeys();

        if (keys.isEmpty()) {
            return;
        }

        String victim =
                null;

        long oldest =
                Long.MAX_VALUE;

        for (String key : keys) {

            long order =
                    accessOrder.getOrDefault(
                            key,
                            Long.MIN_VALUE
                    );

            if (order < oldest) {

                oldest =
                        order;

                victim =
                        key;
            }
        }

        if (victim != null) {

            removeKeyInternal(
                    victim
            );

            evictedKeys.incrementAndGet();

            KeyVersionManager.touch(
                    victim
            );

            recordAof(
                    "DEL "
                            + victim
            );

            System.out.println(
                    "LRU Evicted key: "
                            + victim
            );
        }
    }

    // =========================================================
    // SAVE SNAPSHOT
    // =========================================================

    private static boolean saveSnapshot() {

        synchronized (PERSISTENCE_LOCK) {

            Snapshot snapshot =
                    new Snapshot();

            snapshot.strings.putAll(
                    database
            );

            snapshot.expirations.putAll(
                    expirations
            );

            snapshot.accessOrder.putAll(
                    accessOrder
            );

            snapshot.maxKeys =
                    maxKeys;

            snapshot.accessCounter =
                    accessCounter.get();

            snapshot.evictedKeys =
                    evictedKeys.get();

            snapshot.replicaAppliedSequence =
                    replicaAppliedSequence.get();

            for (
                    Map.Entry<
                            String,
                            ConcurrentLinkedDeque<String>
                            > entry :
                    lists.entrySet()
            ) {

                snapshot.lists.put(
                        entry.getKey(),
                        new ArrayList<>(
                                entry.getValue()
                        )
                );
            }

            for (
                    Map.Entry<
                            String,
                            Set<String>
                            > entry :
                    sets.entrySet()
            ) {

                snapshot.sets.put(
                        entry.getKey(),
                        new HashSet<>(
                                entry.getValue()
                        )
                );
            }

            for (
                    Map.Entry<
                            String,
                            ConcurrentHashMap<String, String>
                            > entry :
                    hashes.entrySet()
            ) {

                snapshot.hashes.put(
                        entry.getKey(),
                        new HashMap<>(
                                entry.getValue()
                        )
                );
            }

            for (
                    Map.Entry<
                            String,
                            SortedSetStore
                            > entry :
                    sortedSets.entrySet()
            ) {

                snapshot.sortedSets.put(
                        entry.getKey(),
                        entry.getValue().toMap()
                );
            }

            try (
                    ObjectOutputStream output =
                            new ObjectOutputStream(
                                    new FileOutputStream(
                                            DATA_FILE
                                    )
                            )
            ) {

                output.writeObject(
                        snapshot
                );

                output.flush();

                System.out.println(
                        "Snapshot saved successfully."
                );

                return true;

            } catch (
                    IOException e
            ) {

                System.out.println(
                        "Snapshot save error: "
                                + e.getMessage()
                );

                return false;
            }
        }
    }

    // =========================================================
    // CHECKPOINT
    // =========================================================

    private static boolean checkpoint() {

        boolean success =
                saveSnapshot();

        if (success) {

            AofManager.clear();

            System.out.println(
                    "Checkpoint complete."
            );
        }

        return success;
    }

    // =========================================================
    // LOAD SNAPSHOT
    // =========================================================

    private static void loadSnapshot() {

        File file =
                new File(
                        DATA_FILE
                );

        if (!file.exists()) {

            System.out.println(
                    "No snapshot found."
            );

            return;
        }

        synchronized (PERSISTENCE_LOCK) {

            try (
                    ObjectInputStream input =
                            new ObjectInputStream(
                                    new FileInputStream(
                                            file
                                    )
                            )
            ) {

                Snapshot snapshot =
                        (Snapshot)
                                input.readObject();

                clearAllDataInternal();

                if (snapshot.strings != null) {

                    database.putAll(
                            snapshot.strings
                    );
                }

                if (snapshot.expirations != null) {

                    expirations.putAll(
                            snapshot.expirations
                    );
                }

                if (snapshot.accessOrder != null) {

                    accessOrder.putAll(
                            snapshot.accessOrder
                    );
                }

                if (snapshot.lists != null) {

                    for (
                            Map.Entry<
                                    String,
                                    List<String>
                                    > entry :
                            snapshot.lists.entrySet()
                    ) {

                        lists.put(
                                entry.getKey(),
                                new ConcurrentLinkedDeque<>(
                                        entry.getValue()
                                )
                        );
                    }
                }

                if (snapshot.sets != null) {

                    for (
                            Map.Entry<
                                    String,
                                    Set<String>
                                    > entry :
                            snapshot.sets.entrySet()
                    ) {

                        Set<String> set =
                                ConcurrentHashMap
                                        .newKeySet();

                        set.addAll(
                                entry.getValue()
                        );

                        sets.put(
                                entry.getKey(),
                                set
                        );
                    }
                }

                if (snapshot.hashes != null) {

                    for (
                            Map.Entry<
                                    String,
                                    Map<String, String>
                                    > entry :
                            snapshot.hashes.entrySet()
                    ) {

                        hashes.put(
                                entry.getKey(),
                                new ConcurrentHashMap<>(
                                        entry.getValue()
                                )
                        );
                    }
                }

                if (snapshot.sortedSets != null) {

                    for (
                            Map.Entry<
                                    String,
                                    Map<String, Double>
                                    > entry :
                            snapshot.sortedSets.entrySet()
                    ) {

                        SortedSetStore store =
                                new SortedSetStore();

                        store.loadFromMap(
                                entry.getValue()
                        );

                        sortedSets.put(
                                entry.getKey(),
                                store
                        );
                    }
                }

                maxKeys =
                        snapshot.maxKeys > 0
                                ?
                        snapshot.maxKeys
                                :
                        DEFAULT_MAX_KEYS;

                accessCounter.set(
                        snapshot.accessCounter
                );

                evictedKeys.set(
                        snapshot.evictedKeys
                );

                replicaAppliedSequence.set(
                        snapshot.replicaAppliedSequence
                );

                for (
                        String key :
                        getAllKeys()
                ) {

                    if (
                            !accessOrder.containsKey(
                                    key
                            )
                    ) {

                        touchKey(
                                key
                        );
                    }
                }

                System.out.println(
                        "Snapshot loaded successfully."
                );

                System.out.println(
                        "Loaded keys: "
                                + getAllKeys().size()
                );

                System.out.println(
                        "Loaded database sequence: "
                                + replicaAppliedSequence.get()
                );

            } catch (
                    IOException
                    |
                    ClassNotFoundException e
            ) {

                System.out.println(
                        "Snapshot load error: "
                                + e.getMessage()
                );
            }
        }
    }

    // =========================================================
    // RESP NULL ARRAY
    // =========================================================

    private static void writeNullArray(
            OutputStream output
    ) throws IOException {

        output.write(
                "*-1\r\n".getBytes(
                        StandardCharsets.UTF_8
                )
        );

        output.flush();
    }

    // =========================================================
    // SEND CLIENT RESPONSE
    // =========================================================

    private static void sendResponse(
            String command,
            String response,
            OutputStream output
    ) throws IOException {

        String operation =
                getOperation(
                        command
                );

        if (
                response.startsWith(
                        "ERROR"
                )
        ) {

            RespProtocol.writeError(
                    output,
                    response.replaceFirst(
                            "^ERROR:\\s*",
                            ""
                    )
            );

            return;
        }

        if (
                operation.equals(
                        "CONFIG"
                )
        ) {

            String[] p =
                    command.split("\\s+");

            if (
                    p.length >= 2
                            &&
                    p[1].equalsIgnoreCase(
                            "GET"
                    )
            ) {

                RespProtocol.writeArray(
                        output,
                        Arrays.asList(
                                response.split(
                                        ", "
                                )
                        )
                );

            } else {

                RespProtocol.writeSimpleString(
                        output,
                        response
                );
            }

            return;
        }

        if (
                operation.equals("PING")
                        ||
                operation.equals("SET")
                        ||
                operation.equals("SETEX")
                        ||
                operation.equals("SAVE")
                        ||
                operation.equals("FLUSHALL")
                        ||
                operation.equals("TYPE")
                        ||
                operation.equals("PROMOTE")
        ) {

            RespProtocol.writeSimpleString(
                    output,
                    response
            );

            return;
        }

        if (
                operation.equals("DEL")
                        ||
                operation.equals("EXISTS")
                        ||
                operation.equals("INCR")
                        ||
                operation.equals("DECR")
                        ||
                operation.equals("EXPIRE")
                        ||
                operation.equals("PEXPIREAT")
                        ||
                operation.equals("TTL")
                        ||
                operation.equals("PTTL")
                        ||
                operation.equals("PERSIST")
                        ||
                operation.equals("LPUSH")
                        ||
                operation.equals("RPUSH")
                        ||
                operation.equals("SADD")
                        ||
                operation.equals("SREM")
                        ||
                operation.equals("SISMEMBER")
                        ||
                operation.equals("HSET")
                        ||
                operation.equals("HDEL")
                        ||
                operation.equals("HEXISTS")
                        ||
                operation.equals("ZADD")
                        ||
                operation.equals("ZREM")
                        ||
                operation.equals("ZCARD")
                        ||
                operation.equals("DBSIZE")
                        ||
                operation.equals("PUBLISH")
        ) {

            RespProtocol.writeInteger(
                    output,
                    Long.parseLong(
                            response
                    )
            );

            return;
        }

        if (
                response.equals(
                        "(nil)"
                )
        ) {

            RespProtocol.writeBulkString(
                    output,
                    null
            );

            return;
        }

        if (
                operation.equals("KEYS")
                        ||
                operation.equals("LRANGE")
                        ||
                operation.equals("SMEMBERS")
                        ||
                operation.equals("ZRANGE")
        ) {

            if (
                    response.equals(
                            "(empty)"
                    )
            ) {

                RespProtocol.writeArray(
                        output,
                        new ArrayList<>()
                );

            } else {

                RespProtocol.writeArray(
                        output,
                        Arrays.asList(
                                response.split(
                                        ", "
                                )
                        )
                );
            }

            return;
        }

        if (
                operation.equals(
                        "HGETALL"
                )
        ) {

            if (
                    response.equals(
                            "(empty)"
                    )
            ) {

                RespProtocol.writeArray(
                        output,
                        new ArrayList<>()
                );

                return;
            }

            List<String> result =
                    new ArrayList<>();

            for (
                    String entry :
                    response.split(
                            ", "
                    )
            ) {

                String[] pair =
                        entry.split(
                                "=",
                                2
                        );

                result.add(
                        pair[0]
                );

                if (pair.length > 1) {

                    result.add(
                            pair[1]
                    );
                }
            }

            RespProtocol.writeArray(
                    output,
                    result
            );

            return;
        }

        RespProtocol.writeBulkString(
                output,
                response
        );
    }
}