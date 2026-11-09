import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public final class SentinelLite {

    // =========================================================
    // SECURITY
    // =========================================================

    private static final String PASSWORD =
            System.getenv()
                    .getOrDefault(
                            "JAVA_REDIS_PASSWORD",
                            "redis123"
                    );

    // =========================================================
    // CONFIG
    // =========================================================

    private static final String HOST =
            "localhost";

    private static final long CHECK_INTERVAL_MS =
            2000;

    private static final int CONNECT_TIMEOUT_MS =
            1200;

    private static final int SOCKET_TIMEOUT_MS =
            1500;

    private static final int MASTER_FAILURE_THRESHOLD =
            3;

    private static final int PROMOTION_CONFIRM_RETRIES =
            10;

    private static final long PROMOTION_CONFIRM_DELAY_MS =
            500;

    // =========================================================
    // CLUSTER STATE
    // =========================================================

    private static final List<Integer> clusterPorts =
            new ArrayList<>();

    // Persist the last known master across Sentinel restarts.
    private static final String STATE_FILE =
            "sentinel-state.properties";

    private static volatile int currentMasterPort =
            6380;

    private static volatile int consecutiveMasterFailures =
            0;

    private static long successfulChecks =
            0;

    // Stage 26: monitoring dashboard and event history.
    private static final long DASHBOARD_INTERVAL_MS = 10_000;
    private static long lastDashboardTime = 0;
    private static final String EVENT_LOG_FILE = "sentinel-events.log";
    private static final Map<Integer, Boolean> previousNodeReachability =
            new HashMap<>();
    private static final Map<Integer, String> previousNodeRoles =
            new HashMap<>();
    private static final DateTimeFormatter EVENT_TIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private SentinelLite() {
    }

    // =========================================================
    // NODE STATUS
    // =========================================================

    private static final class NodeStatus {

        private final int port;

        private final boolean reachable;

        private final String role;

        private final long sequence;

        private final String error;

        private NodeStatus(
                int port,
                boolean reachable,
                String role,
                long sequence,
                String error
        ) {

            this.port =
                    port;

            this.reachable =
                    reachable;

            this.role =
                    role;

            this.sequence =
                    sequence;

            this.error =
                    error;
        }

        private static NodeStatus online(
                int port,
                String role,
                long sequence
        ) {

            return new NodeStatus(
                    port,
                    true,
                    role,
                    sequence,
                    null
            );
        }

        private static NodeStatus offline(
                int port,
                String error
        ) {

            return new NodeStatus(
                    port,
                    false,
                    "UNKNOWN",
                    -1,
                    error
            );
        }

        public int getPort() {

            return port;
        }

        public boolean isReachable() {

            return reachable;
        }

        public String getRole() {

            return role;
        }

        public long getSequence() {

            return sequence;
        }

        public String getError() {

            return error;
        }

        public boolean isMaster() {

            return reachable
                    &&
                    "MASTER".equalsIgnoreCase(
                            role
                    );
        }

        public boolean isReplica() {

            return reachable
                    &&
                    "REPLICA".equalsIgnoreCase(
                            role
                    );
        }

        @Override
        public String toString() {

            return
                    HOST
                            + ":"
                            + port
                            + " role="
                            + role
                            + " reachable="
                            + reachable
                            + " sequence="
                            + sequence;
        }
    }

    // =========================================================
    // MAIN
    // =========================================================

    public static void main(
            String[] args
    ) {

        configure(
                args
        );

        printStartup();

        while (true) {

            try {

                monitorMaster();
                monitorClusterDashboard();

                Thread.sleep(
                        CHECK_INTERVAL_MS
                );

            } catch (
                    InterruptedException e
            ) {

                Thread.currentThread()
                        .interrupt();

                System.out.println(
                        "Sentinel stopped."
                );

                return;

            } catch (
                    Exception e
            ) {

                System.out.println(
                        "[SENTINEL ERROR] "
                                + e.getMessage()
                );

                try {

                    Thread.sleep(
                            CHECK_INTERVAL_MS
                    );

                } catch (
                        InterruptedException interrupted
                ) {

                    Thread.currentThread()
                            .interrupt();

                    return;
                }
            }
        }
    }

    // =========================================================
    // CONFIGURE
    // =========================================================

    private static void configure(
            String[] args
    ) {

        clusterPorts.clear();

        /*
         * Usage:
         *
         * java SentinelLite
         *
         * or:
         *
         * java SentinelLite 6380 6380 6381 6382
         *
         * First argument = current master
         * remaining arguments = cluster ports
         */

        if (
                args == null
                        ||
                args.length == 0
        ) {

            // No command-line override: restore the last saved master.
            currentMasterPort = loadPersistedMasterPort();

            clusterPorts.add(6380);
            clusterPorts.add(6381);
            clusterPorts.add(6382);

            if (!clusterPorts.contains(currentMasterPort)) {
                clusterPorts.add(currentMasterPort);
            }

            Collections.sort(clusterPorts);
            return;
        }

        // An explicit first argument overrides the saved state.
        currentMasterPort =
                Integer.parseInt(
                        args[0]
                );

        LinkedHashSet<Integer> uniquePorts =
                new LinkedHashSet<>();

        if (args.length >= 2) {

            for (
                    int i = 1;
                    i < args.length;
                    i++
            ) {

                uniquePorts.add(
                        Integer.parseInt(
                                args[i]
                        )
                );
            }

        } else {

            uniquePorts.add(
                    6380
            );

            uniquePorts.add(
                    6381
            );

            uniquePorts.add(
                    6382
            );
        }

        uniquePorts.add(
                currentMasterPort
        );

        clusterPorts.addAll(
                uniquePorts
        );

        Collections.sort(
                clusterPorts
        );

        // Save the explicitly supplied master so a later plain restart can restore it.
        saveSentinelState();
    }

    // =========================================================
    // SENTINEL STATE PERSISTENCE
    // =========================================================

    private static int loadPersistedMasterPort() {

        File file = new File(STATE_FILE);

        if (!file.exists()) {
            System.out.println(
                    "No saved Sentinel state found. Using default master 6380."
            );
            return 6380;
        }

        Properties properties = new Properties();

        try (FileInputStream input = new FileInputStream(file)) {

            properties.load(input);

            int port = Integer.parseInt(
                    properties.getProperty("master_port", "6380").trim()
            );

            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("Invalid saved master port: " + port);
            }

            System.out.println(
                    "Persisted master loaded: " + port
            );

            return port;

        } catch (Exception e) {

            System.out.println(
                    "[SENTINEL STATE LOAD ERROR] " + e.getMessage()
                            + ". Using default master 6380."
            );

            return 6380;
        }
    }

    private static void saveSentinelState() {

        Properties properties = new Properties();

        properties.setProperty(
                "master_port",
                String.valueOf(currentMasterPort)
        );

        try (FileOutputStream output = new FileOutputStream(STATE_FILE)) {

            properties.store(
                    output,
                    "Java Redis Sentinel State"
            );

            System.out.println(
                    "[SENTINEL STATE SAVED] Master = " + currentMasterPort
            );

        } catch (IOException e) {

            System.out.println(
                    "[SENTINEL STATE SAVE ERROR] " + e.getMessage()
            );
        }
    }

    // =========================================================
    // STARTUP
    // =========================================================

    private static void printStartup() {

        System.out.println();
        System.out.println(
                "========================================"
        );

        System.out.println(
                "JAVA REDIS SENTINEL-LITE"
        );

        System.out.println(
                "========================================"
        );

        System.out.println(
                "Monitoring Master = "
                        + HOST
                        + ":"
                        + currentMasterPort
        );

        System.out.println(
                "Cluster Nodes = "
                        + clusterPorts
        );

        System.out.println(
                "Check Interval = "
                        + CHECK_INTERVAL_MS
                        + " ms"
        );

        System.out.println(
                "Failure Threshold = "
                        + MASTER_FAILURE_THRESHOLD
        );

        System.out.println(
                "Automatic Promotion = enabled"
        );

        System.out.println(
                "Candidate Strategy = highest replication sequence"
        );

        System.out.println(
                "Failover Safety = cluster majority quorum"
        );

        System.out.println(
                "Split-Brain Guard = enabled"
        );

        System.out.println(
                "========================================"
        );

        System.out.println();
    }

    // =========================================================
    // MONITOR MASTER
    // =========================================================

    private static void monitorMaster() {

        NodeStatus master =
                inspectNode(
                        currentMasterPort
                );

        if (
                master.isReachable()
                        &&
                master.isMaster()
        ) {

            consecutiveMasterFailures =
                    0;

            successfulChecks++;

            /*
             * Don't print every heartbeat.
             * Print approximately every 10 seconds.
             */
            if (
                    successfulChecks % 5
                            ==
                    0
            ) {

                System.out.println(
                        "[MASTER OK] "
                                + HOST
                                + ":"
                                + currentMasterPort
                                + " sequence="
                                + master.getSequence()
                );
            }

            return;
        }

        consecutiveMasterFailures++;

        System.out.println(
                "[MASTER WARNING] "
                        + HOST
                        + ":"
                        + currentMasterPort
                        + " failure "
                        + consecutiveMasterFailures
                        + "/"
                        + MASTER_FAILURE_THRESHOLD
        );

        if (
                master.getError()
                        != null
        ) {

            System.out.println(
                    "Reason: "
                            + master.getError()
            );

        } else {

            System.out.println(
                    "Reason: node is reachable but role="
                            + master.getRole()
            );
        }

        if (
                consecutiveMasterFailures
                        <
                MASTER_FAILURE_THRESHOLD
        ) {

            return;
        }

        System.out.println();
        System.out.println(
                "MASTER FAILURE CONFIRMED."
        );

        System.out.println(
                "Starting automatic failover..."
        );

        boolean success =
                performAutomaticFailover();

        if (success) {

            consecutiveMasterFailures =
                    0;

            successfulChecks =
                    0;

        } else {

            System.out.println(
                    "Automatic failover could not be completed."
            );
        }
    }

    // =========================================================
    // AUTOMATIC FAILOVER
    // =========================================================

    private static boolean performAutomaticFailover() {

        List<NodeStatus> candidates =
                new ArrayList<>();

        // Stage 27: require a majority of configured Redis nodes to be reachable.
        // This is a cluster-node quorum safety gate, not a multi-Sentinel quorum.
        int reachableNodes = 0;

        for (int port : clusterPorts) {
            if (inspectNode(port).isReachable()) {
                reachableNodes++;
            }
        }

        int requiredQuorum = (clusterPorts.size() / 2) + 1;

        System.out.println();
        System.out.println(
                "CLUSTER QUORUM CHECK: " + reachableNodes
                        + "/" + clusterPorts.size()
                        + " nodes reachable; required=" + requiredQuorum
        );

        if (reachableNodes < requiredQuorum) {
            String message = "Failover blocked: cluster quorum not reached ("
                    + reachableNodes + "/" + requiredQuorum + " reachable).";
            System.out.println("[QUORUM REJECTED] " + message);
            logEvent("QUORUM_REJECTED", message);
            return false;
        }

        System.out.println("[QUORUM PASSED] Majority availability confirmed.");
        logEvent("QUORUM_PASSED", "Reachable nodes=" + reachableNodes
                + ", required=" + requiredQuorum);

        System.out.println();
        System.out.println(
                "Scanning replica candidates..."
        );

        for (
                int port :
                clusterPorts
        ) {

            if (
                    port
                            ==
                    currentMasterPort
            ) {

                continue;
            }

            NodeStatus status =
                    inspectNode(
                            port
                    );

            if (!status.isReachable()) {

                System.out.println(
                        HOST
                                + ":"
                                + port
                                + " -> OFFLINE"
                );

                continue;
            }

            System.out.println(
                    HOST
                            + ":"
                            + port
                            + " -> "
                            + status.getRole()
                            + " sequence="
                            + status.getSequence()
            );

            // A second reachable master means the cluster may already be split-brain.
            if (status.isMaster()) {
                String message = "Failover blocked: unexpected MASTER detected on port "
                        + port + ". Resolve the role conflict before promotion.";
                System.out.println("[SPLIT-BRAIN GUARD] " + message);
                logEvent("SPLIT_BRAIN_GUARD", message);
                return false;
            }

            if (
                    status.isReplica()
                            &&
                    status.getSequence()
                            >=
                    0
            ) {

                candidates.add(
                        status
                );
            }
        }

        if (candidates.isEmpty()) {

            System.out.println(
                    "No healthy replica candidates available."
            );

            return false;
        }

        // =====================================================
        // SELECT BEST CANDIDATE
        // =====================================================
        //
        // 1. Highest sequence
        // 2. Lowest port if tied
        // =====================================================

        candidates.sort(
                Comparator
                        .comparingLong(
                                NodeStatus::getSequence
                        )
                        .reversed()

                        .thenComparingInt(
                                NodeStatus::getPort
                        )
        );

        NodeStatus selected =
                candidates.get(0);

        System.out.println();
        System.out.println(
                "Selected candidate:"
        );

        System.out.println(
                HOST
                        + ":"
                        + selected.getPort()
        );

        System.out.println(
                "Sequence = "
                        + selected.getSequence()
        );

        // =====================================================
        // PROMOTE
        // =====================================================

        String response;

        try {

            response =
                    executeCommand(
                            selected.getPort(),
                            "PROMOTE"
                    );

        } catch (
                IOException e
        ) {

            System.out.println(
                    "Promotion command failed: "
                            + e.getMessage()
            );

            return false;
        }

        System.out.println(
                "Promotion response = "
                        + response
        );

        if (
                response == null
                        ||
                !response.toUpperCase()
                        .startsWith(
                                "OK"
                        )
        ) {

            System.out.println(
                    "Candidate rejected promotion."
            );

            return false;
        }

        // =====================================================
        // CONFIRM PROMOTION
        // =====================================================

        for (
                int attempt = 1;
                attempt
                        <=
                PROMOTION_CONFIRM_RETRIES;
                attempt++
        ) {

            try {

                Thread.sleep(
                        PROMOTION_CONFIRM_DELAY_MS
                );

            } catch (
                    InterruptedException e
            ) {

                Thread.currentThread()
                        .interrupt();

                return false;
            }

            NodeStatus confirmation =
                    inspectNode(
                            selected.getPort()
                    );

            if (
                    confirmation.isReachable()
                            &&
                    confirmation.isMaster()
            ) {

                // Verify no other reachable node is also reporting MASTER.
                boolean conflictingMaster = false;
                for (int port : clusterPorts) {
                    if (port == selected.getPort()) {
                        continue;
                    }
                    NodeStatus other = inspectNode(port);
                    if (other.isMaster()) {
                        conflictingMaster = true;
                        String message = "Promotion verification failed: another MASTER is online at port "
                                + port + ".";
                        System.out.println("[SPLIT-BRAIN GUARD] " + message);
                        logEvent("SPLIT_BRAIN_GUARD", message);
                    }
                }

                if (conflictingMaster) {
                    return false;
                }

                int oldMaster =
                        currentMasterPort;

                currentMasterPort =
                        selected.getPort();

                saveSentinelState();
                logEvent(
                        "FAILOVER",
                        "Master changed from " + oldMaster
                                + " to " + currentMasterPort
                                + " (sequence=" + confirmation.getSequence() + ")"
                );

                System.out.println();
                System.out.println(
                        "========================================"
                );

                System.out.println(
                        "AUTOMATIC FAILOVER SUCCESSFUL"
                );

                System.out.println(
                        "Old Master = "
                                + HOST
                                + ":"
                                + oldMaster
                );

                System.out.println(
                        "New Master = "
                                + HOST
                                + ":"
                                + currentMasterPort
                );

                System.out.println(
                        "New Master Sequence = "
                                + confirmation.getSequence()
                );

                System.out.println(
                        "========================================"
                );

                System.out.println();

                return true;
            }
        }

        System.out.println(
                "Promotion could not be confirmed."
        );

        return false;
    }

    // =========================================================
    // STAGE 26: CLUSTER HEALTH DASHBOARD
    // =========================================================

    private static void monitorClusterDashboard() {

        long now = System.currentTimeMillis();

        if (now - lastDashboardTime < DASHBOARD_INTERVAL_MS) {
            return;
        }

        lastDashboardTime = now;

        List<NodeStatus> statuses = new ArrayList<>();
        int online = 0;
        int offline = 0;
        long masterSequence = -1;

        for (int port : clusterPorts) {
            NodeStatus status = inspectNode(port);
            statuses.add(status);

            if (status.isReachable()) {
                online++;

                if (status.isMaster()) {
                    masterSequence = status.getSequence();
                }
            } else {
                offline++;
            }

            Boolean wasReachable = previousNodeReachability.put(
                    port, status.isReachable()
            );

            String oldRole = previousNodeRoles.put(port, status.getRole());

            if (wasReachable != null && !wasReachable && status.isReachable()) {
                String message = "Node " + port + " recovered; role="
                        + status.getRole() + ", sequence=" + status.getSequence();
                System.out.println("[RECOVERY ALERT] " + message);
                logEvent("NODE_RECOVERY", message);
            }

            if (wasReachable != null && wasReachable && !status.isReachable()) {
                String message = "Node " + port + " became OFFLINE; reason="
                        + status.getError();
                System.out.println("[NODE ALERT] " + message);
                logEvent("NODE_OFFLINE", message);
            }

            if (oldRole != null && status.isReachable()
                    && !oldRole.equalsIgnoreCase(status.getRole())) {
                String message = "Node " + port + " role changed: "
                        + oldRole + " -> " + status.getRole();
                System.out.println("[ROLE CHANGE] " + message);
                logEvent("ROLE_CHANGE", message);
            }
        }

        System.out.println();
        System.out.println("==============================================================");
        System.out.println("                  REDIS CLUSTER HEALTH");
        System.out.println("==============================================================");
        System.out.printf("%-8s %-12s %-12s %-12s %-12s%n",
                "PORT", "ROLE", "STATUS", "SEQUENCE", "REPLICA LAG");

        for (NodeStatus status : statuses) {
            String state = status.isReachable() ? "ONLINE" : "OFFLINE";
            String lag = "-";

            if (status.isReachable() && status.isReplica()
                    && masterSequence >= 0 && status.getSequence() >= 0) {
                lag = String.valueOf(Math.max(0,
                        masterSequence - status.getSequence()));
            }

            String sequence = status.getSequence() >= 0
                    ? String.valueOf(status.getSequence()) : "-";

            System.out.printf("%-8d %-12s %-12s %-12s %-12s%n",
                    status.getPort(), status.getRole(), state, sequence, lag);
        }

        System.out.println("--------------------------------------------------------------");
        System.out.println("Cluster Nodes : " + clusterPorts.size());
        System.out.println("Online Nodes  : " + online);
        System.out.println("Offline Nodes : " + offline);
        System.out.println("Current Master: " + HOST + ":" + currentMasterPort);
        System.out.println("Event Log     : " + EVENT_LOG_FILE);
        System.out.println("==============================================================");
        System.out.println();
    }

    // Append monitoring events without stopping Sentinel if logging fails.
    private static void logEvent(String eventType, String message) {
        String timestamp = LocalDateTime.now().format(EVENT_TIME_FORMAT);

        try (FileWriter writer = new FileWriter(EVENT_LOG_FILE, true)) {
            writer.write(timestamp + " [" + eventType + "] " + message
                    + System.lineSeparator());
        } catch (IOException e) {
            System.out.println("[EVENT LOG ERROR] " + e.getMessage());
        }
    }

    // =========================================================
    // INSPECT NODE
    // =========================================================

    private static NodeStatus inspectNode(
            int port
    ) {

        try (
                Socket socket =
                        openSocket(
                                port
                        )
        ) {

            InputStream input =
                    socket.getInputStream();

            OutputStream output =
                    socket.getOutputStream();

            authenticate(
                    input,
                    output
            );

            // =============================================
            // PING
            // =============================================

            sendCommand(
                    output,
                    "PING"
            );

            Object pong =
                    readResp(
                            input
                    );

            if (
                    pong == null
                            ||
                    !"PONG".equalsIgnoreCase(
                            String.valueOf(
                                    pong
                            )
                    )
            ) {

                throw new IOException(
                        "PING failed"
                );
            }

            // =============================================
            // INFO
            // =============================================

            sendCommand(
                    output,
                    "INFO"
            );

            Object infoResponse =
                    readResp(
                            input
                    );

            String info =
                    infoResponse == null
                            ?
                            ""
                            :
                            String.valueOf(
                                    infoResponse
                            );

            String role =
                    getField(
                            info,
                            "role"
                    );

            if (
                    role == null
                            ||
                    role.isBlank()
            ) {

                role =
                        "UNKNOWN";
            }

            role =
                    role.toUpperCase();

            // =============================================
            // REPLICATIONINFO
            // =============================================

            sendCommand(
                    output,
                    "REPLICATIONINFO"
            );

            Object replicationResponse =
                    readResp(
                            input
                    );

            String replicationInfo =
                    replicationResponse == null
                            ?
                            ""
                            :
                            String.valueOf(
                                    replicationResponse
                            );

            long sequence;

            if (
                    "MASTER".equalsIgnoreCase(
                            role
                    )
            ) {

                sequence =
                        getLongField(
                                replicationInfo,
                                "master_sequence",
                                getLongField(
                                        info,
                                        "replica_applied_sequence",
                                        0
                                )
                        );

            } else {

                sequence =
                        getLongField(
                                replicationInfo,
                                "replica_applied_sequence",
                                getLongField(
                                        info,
                                        "replica_applied_sequence",
                                        -1
                                )
                        );
            }

            return NodeStatus.online(
                    port,
                    role,
                    sequence
            );

        } catch (
                Exception e
        ) {

            return NodeStatus.offline(
                    port,
                    e.getMessage()
            );
        }
    }

    // =========================================================
    // EXECUTE ONE COMMAND
    // =========================================================

    private static String executeCommand(
            int port,
            String... command
    ) throws IOException {

        try (
                Socket socket =
                        openSocket(
                                port
                        )
        ) {

            InputStream input =
                    socket.getInputStream();

            OutputStream output =
                    socket.getOutputStream();

            authenticate(
                    input,
                    output
            );

            sendCommand(
                    output,
                    command
            );

            Object response =
                    readResp(
                            input
                    );

            return response == null
                    ?
                    null
                    :
                    String.valueOf(
                            response
                    );
        }
    }

    // =========================================================
    // OPEN SOCKET
    // =========================================================

    private static Socket openSocket(
            int port
    ) throws IOException {

        Socket socket =
                new Socket();

        socket.connect(
                new InetSocketAddress(
                        HOST,
                        port
                ),
                CONNECT_TIMEOUT_MS
        );

        socket.setSoTimeout(
                SOCKET_TIMEOUT_MS
        );

        socket.setTcpNoDelay(
                true
        );

        return socket;
    }

    // =========================================================
    // AUTH
    // =========================================================

    private static void authenticate(
            InputStream input,
            OutputStream output
    ) throws IOException {

        sendCommand(
                output,
                "AUTH",
                PASSWORD
        );

        Object response =
                readResp(
                        input
                );

        if (
                response == null
                        ||
                !"OK".equalsIgnoreCase(
                        String.valueOf(
                                response
                        )
                )
        ) {

            throw new IOException(
                    "Authentication failed"
            );
        }
    }

    // =========================================================
    // SEND RESP COMMAND
    // =========================================================

    private static void sendCommand(
            OutputStream output,
            String... arguments
    ) throws IOException {

        if (
                arguments == null
                        ||
                arguments.length == 0
        ) {

            throw new IllegalArgumentException(
                    "Command cannot be empty"
            );
        }

        StringBuilder builder =
                new StringBuilder();

        builder.append("*")
                .append(
                        arguments.length
                )
                .append("\r\n");

        for (
                String argument :
                arguments
        ) {

            if (argument == null) {

                argument =
                        "";
            }

            byte[] bytes =
                    argument.getBytes(
                            StandardCharsets.UTF_8
                    );

            builder.append("$")
                    .append(
                            bytes.length
                    )
                    .append("\r\n");

            builder.append(
                    argument
            );

            builder.append(
                    "\r\n"
            );
        }

        output.write(
                builder.toString()
                        .getBytes(
                                StandardCharsets.UTF_8
                        )
        );

        output.flush();
    }

    // =========================================================
    // READ RESP
    // =========================================================

    private static Object readResp(
            InputStream input
    ) throws IOException {

        int type =
                input.read();

        if (type == -1) {

            throw new EOFException(
                    "Connection closed"
            );
        }

        switch (type) {

            // Simple string
            case '+':

                return readLine(
                        input
                );

            // Error
            case '-':

                throw new IOException(
                        readLine(
                                input
                        )
                );

            // Integer
            case ':':

                return Long.parseLong(
                        readLine(
                                input
                        )
                );

            // Bulk string
            case '$': {

                int length =
                        Integer.parseInt(
                                readLine(
                                        input
                                )
                        );

                if (length < 0) {

                    return null;
                }

                byte[] data =
                        readExactly(
                                input,
                                length
                        );

                expectCrLf(
                        input
                );

                return new String(
                        data,
                        StandardCharsets.UTF_8
                );
            }

            // Array
            case '*': {

                int count =
                        Integer.parseInt(
                                readLine(
                                        input
                                )
                        );

                if (count < 0) {

                    return null;
                }

                List<Object> result =
                        new ArrayList<>();

                for (
                        int i = 0;
                        i < count;
                        i++
                ) {

                    result.add(
                            readResp(
                                    input
                            )
                    );
                }

                return result;
            }

            default:

                throw new IOException(
                        "Unknown RESP type: "
                                + (char) type
                );
        }
    }

    // =========================================================
    // READ LINE
    // =========================================================

    private static String readLine(
            InputStream input
    ) throws IOException {

        ByteArrayOutputStream buffer =
                new ByteArrayOutputStream();

        while (true) {

            int value =
                    input.read();

            if (value == -1) {

                throw new EOFException(
                        "Unexpected end of stream"
                );
            }

            if (value == '\n') {

                break;
            }

            if (value != '\r') {

                buffer.write(
                        value
                );
            }
        }

        return buffer.toString(
                StandardCharsets.UTF_8
        );
    }

    // =========================================================
    // READ EXACT
    // =========================================================

    private static byte[] readExactly(
            InputStream input,
            int length
    ) throws IOException {

        byte[] data =
                new byte[length];

        int offset =
                0;

        while (
                offset
                        <
                length
        ) {

            int count =
                    input.read(
                            data,
                            offset,
                            length - offset
                    );

            if (count == -1) {

                throw new EOFException(
                        "Unexpected end of stream"
                );
            }

            offset +=
                    count;
        }

        return data;
    }

    // =========================================================
    // CRLF
    // =========================================================

    private static void expectCrLf(
            InputStream input
    ) throws IOException {

        int first =
                input.read();

        int second =
                input.read();

        if (
                first != '\r'
                        ||
                second != '\n'
        ) {

            throw new IOException(
                    "Invalid RESP CRLF"
            );
        }
    }

    // =========================================================
    // FIELD PARSER
    // =========================================================

    private static String getField(
            String text,
            String field
    ) {

        if (
                text == null
                        ||
                field == null
        ) {

            return null;
        }

        String prefix =
                field
                        + ":";

        String[] lines =
                text.split(
                        "\\r?\\n"
                );

        for (
                String line :
                lines
        ) {

            if (
                    line.startsWith(
                            prefix
                    )
            ) {

                return line.substring(
                        prefix.length()
                ).trim();
            }
        }

        return null;
    }

    // =========================================================
    // LONG FIELD
    // =========================================================

    private static long getLongField(
            String text,
            String field,
            long defaultValue
    ) {

        String value =
                getField(
                        text,
                        field
                );

        if (
                value == null
                        ||
                value.isBlank()
        ) {

            return defaultValue;
        }

        try {

            return Long.parseLong(
                    value
            );

        } catch (
                NumberFormatException e
        ) {

            return defaultValue;
        }
    }
}