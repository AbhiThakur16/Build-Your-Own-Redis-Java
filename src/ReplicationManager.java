import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public final class ReplicationManager {

    // =========================================================
    // FULL SYNC PROVIDER
    // =========================================================

    public interface FullSyncProvider {
        FullSyncPayload createPayload();
    }

    private static volatile FullSyncProvider fullSyncProvider;

    // =========================================================
    // SECURITY
    // =========================================================

    private static final String PASSWORD =
            System.getenv()
                    .getOrDefault(
                            "JAVA_REDIS_PASSWORD",
                            "redis123"
                    );

    private static final String REPLICATION_TOKEN =
            System.getenv()
                    .getOrDefault(
                            "JAVA_REDIS_REPLICATION_TOKEN",
                            "redis-replica-123"
                    );

    // =========================================================
    // CONFIG
    // =========================================================

    private static final long HEARTBEAT_INTERVAL_MS =
            3000;

    private static final int SOCKET_TIMEOUT_MS =
            2500;

    private static final int CONNECT_TIMEOUT_MS =
            2000;

    private static final long RECONNECT_DELAY_MS =
            1000;

    // =========================================================
    // GLOBAL STATE
    // =========================================================

    private static volatile boolean enabled =
            false;

    private static volatile boolean running =
            false;

    private static final AtomicLong replicationSequence =
            new AtomicLong(0);

    private static final AtomicLong skippedCommands =
            new AtomicLong(0);

    private static final ConcurrentHashMap<
            String,
            ReplicaWorker
            > workers =
            new ConcurrentHashMap<>();

    private ReplicationManager() {
    }

    // =========================================================
    // REPLICA STATE
    // =========================================================

    private static final class ReplicaState {

        private final String replicationId;
        private final long epoch;
        private final long sequence;

        private ReplicaState(
                String replicationId,
                long epoch,
                long sequence
        ) {

            this.replicationId =
                    replicationId;

            this.epoch =
                    epoch;

            this.sequence =
                    sequence;
        }
    }

    // =========================================================
    // REPLICATION COMMAND
    // =========================================================

    private static final class ReplicationCommand {

        private final long sequence;
        private final String command;

        private ReplicationCommand(
                long sequence,
                String command
        ) {

            this.sequence =
                    sequence;

            this.command =
                    command;
        }
    }

    // =========================================================
    // REPLICA WORKER
    // =========================================================

    private static final class ReplicaWorker {

        private final ReplicaTarget target;

        private final BlockingQueue<ReplicationCommand> queue =
                new LinkedBlockingQueue<>();

        private volatile boolean workerRunning =
                true;

        private Thread thread;

        private Socket socket;

        private InputStream input;

        private OutputStream output;

        private volatile long lastHeartbeatAttempt =
                0;

        private ReplicaWorker(
                ReplicaTarget target
        ) {

            this.target =
                    target;
        }

        // =====================================================
        // START
        // =====================================================

        private void start() {

            thread =
                    new Thread(
                            this::workerLoop,
                            "replication-"
                                    + target.getPort()
                    );

            thread.setDaemon(
                    true
            );

            thread.start();

            System.out.println(
                    "Replica worker started: "
                            + target.getId()
            );
        }

        // =====================================================
        // ENQUEUE
        // =====================================================

        private void enqueue(
                ReplicationCommand command
        ) {

            if (
                    workerRunning
                            &&
                    command != null
            ) {

                queue.offer(
                        command
                );
            }
        }

        // =====================================================
        // WORKER LOOP
        // =====================================================

        private void workerLoop() {

            while (
                    running
                            &&
                    workerRunning
            ) {

                try {

                    ReplicationCommand item =
                            queue.poll(
                                    250,
                                    TimeUnit.MILLISECONDS
                            );

                    if (item != null) {

                        processReplicationItem(
                                item
                        );
                    }

                    long now =
                            System.currentTimeMillis();

                    if (
                            now
                                    -
                            lastHeartbeatAttempt
                                    >=
                            HEARTBEAT_INTERVAL_MS
                    ) {

                        performHeartbeat();
                    }

                } catch (
                        InterruptedException e
                ) {

                    if (
                            !running
                                    ||
                            !workerRunning
                    ) {

                        return;
                    }
                }
            }
        }

        // =====================================================
        // PROCESS LIVE COMMAND
        // =====================================================

        private void processReplicationItem(
                ReplicationCommand item
        ) throws InterruptedException {

            boolean completed =
                    false;

            while (
                    running
                            &&
                    workerRunning
                            &&
                    !completed
            ) {

                try {

                    ensureConnected();

                    /*
                     * A FULL/PARTIAL sync performed while
                     * reconnecting may already contain this
                     * queued command.
                     */
                    if (
                            item.sequence
                                    <=
                            target.getLastSyncedSequence()
                    ) {

                        skippedCommands
                                .incrementAndGet();

                        completed =
                                true;

                        continue;
                    }

                    sendReplicationCommand(
                            item.sequence,
                            item.command
                    );

                    boolean accepted =
                            readCommandResponse();

                    if (!accepted) {

                        target.incrementFailedCommands();

                        throw new IOException(
                                "Replica rejected replicated command"
                        );
                    }

                    target.incrementCommandsSent();

                    target.setLastSyncedSequence(
                            item.sequence
                    );

                    completed =
                            true;

                } catch (
                        IOException e
                ) {

                    target.incrementReconnects();

                    handleConnectionFailure(
                            "Replication failure: "
                                    + e.getMessage()
                    );

                    if (
                            running
                                    &&
                            workerRunning
                    ) {

                        Thread.sleep(
                                RECONNECT_DELAY_MS
                        );
                    }
                }
            }
        }

        // =====================================================
        // HEARTBEAT
        // =====================================================

        private void performHeartbeat() {

            lastHeartbeatAttempt =
                    System.currentTimeMillis();

            try {

                ensureConnected();

                long start =
                        System.nanoTime();

                sendArguments(
                        Collections.singletonList(
                                "PING"
                        )
                );

                String response =
                        readSimpleResponse();

                long latency =
                        TimeUnit.NANOSECONDS
                                .toMillis(
                                        System.nanoTime()
                                                -
                                        start
                                );

                if (
                        !"PONG".equalsIgnoreCase(
                                response
                        )
                ) {

                    throw new IOException(
                            "Unexpected heartbeat response: "
                                    + response
                    );
                }

                target.recordHeartbeatSuccess(
                        latency
                );

            } catch (
                    IOException e
            ) {

                target.incrementReconnects();

                handleConnectionFailure(
                        "Heartbeat failed: "
                                + e.getMessage()
                );
            }
        }

        // =====================================================
        // CONNECTION FAILURE
        // =====================================================

        private void handleConnectionFailure(
                String reason
        ) {

            target.recordHeartbeatFailure();

            closeTransportOnly();

            if (
                    target.getHealthStatus()
                            ==
                    ReplicaHealth.Status.OFFLINE
            ) {

                target.setConnected(
                        false
                );
            }

            System.out.println(
                    "[HEALTH] "
                            + target.getId()
                            + " -> "
                            + target.getHealthStatus()
                            + " | failures="
                            + target
                                    .getConsecutiveHeartbeatFailures()
            );

            if (
                    reason != null
                            &&
                    !reason.isBlank()
            ) {

                System.out.println(
                        "         "
                                + reason
                );
            }
        }

        // =====================================================
        // ENSURE CONNECTION
        // =====================================================

        private synchronized void ensureConnected()
                throws IOException {

            if (
                    socket != null
                            &&
                    socket.isConnected()
                            &&
                    !socket.isClosed()
                            &&
                    target.isConnected()
            ) {

                return;
            }

            closeTransportOnly();

            Socket newSocket =
                    new Socket();

            newSocket.connect(
                    new InetSocketAddress(
                            target.getHost(),
                            target.getPort()
                    ),
                    CONNECT_TIMEOUT_MS
            );

            newSocket.setTcpNoDelay(
                    true
            );

            newSocket.setSoTimeout(
                    SOCKET_TIMEOUT_MS
            );

            socket =
                    newSocket;

            input =
                    socket.getInputStream();

            output =
                    socket.getOutputStream();

            // ================================================
            // AUTH
            // ================================================

            sendArguments(
                    Arrays.asList(
                            "AUTH",
                            PASSWORD
                    )
            );

            String auth =
                    readSimpleResponse();

            if (
                    !"OK".equalsIgnoreCase(
                            auth
                    )
            ) {

                throw new IOException(
                        "Replica authentication failed"
                );
            }

            // ================================================
            // REPLICATION HANDSHAKE
            // ================================================

            sendArguments(
                    Arrays.asList(
                            "REPLICAHELLO",
                            REPLICATION_TOKEN
                    )
            );

            String hello =
                    readSimpleResponse();

            if (
                    !"OK".equalsIgnoreCase(
                            hello
                    )
            ) {

                throw new IOException(
                        "Replication handshake failed"
                );
            }

            System.out.println(
                    "Replication handshake successful: "
                            + target.getId()
            );

            // ================================================
            // GET REPLICA HISTORY STATE
            // ================================================

            ReplicaState replicaState =
                    requestReplicaState();

            target.setLastReportedSequence(
                    replicaState.sequence
            );

            String masterReplicationId =
                    ReplicationMetadata
                            .getMasterReplicationId();

            long masterEpoch =
                    ReplicationMetadata
                            .getMasterEpoch();

            long masterSequence =
                    replicationSequence.get();

            boolean sameReplicationHistory =
                    ReplicationId.matches(
                            masterReplicationId,
                            replicaState.replicationId
                    )
                            &&
                    masterEpoch
                            ==
                    replicaState.epoch;

            System.out.println(
                    "Replica = "
                            + target.getId()
            );

            System.out.println(
                    "Replica REPL ID = "
                            +
                            (
                                    ReplicationId.isValid(
                                            replicaState.replicationId
                                    )
                                            ?
                                    ReplicationId.shortId(
                                            replicaState.replicationId
                                    ) + "..."
                                            :
                                    "none"
                            )
            );

            System.out.println(
                    "Replica Epoch = "
                            + replicaState.epoch
            );

            System.out.println(
                    "Replica Sequence = "
                            + replicaState.sequence
            );

            System.out.println(
                    "Master REPL ID = "
                            +
                            (
                                    ReplicationId.isValid(
                                            masterReplicationId
                                    )
                                            ?
                                    ReplicationId.shortId(
                                            masterReplicationId
                                    ) + "..."
                                            :
                                    "none"
                            )
            );

            System.out.println(
                    "Master Epoch = "
                            + masterEpoch
            );

            System.out.println(
                    "Master Sequence = "
                            + masterSequence
            );

            System.out.println(
                    "Same Replication History = "
                            + sameReplicationHistory
            );

            // ================================================
            // SYNC DECISION
            // ================================================

            if (
                    sameReplicationHistory
                            &&
                    replicaState.sequence
                            ==
                    masterSequence
            ) {

                target.setLastSyncedSequence(
                        masterSequence
                );

                System.out.println(
                        target.getId()
                                + " already synchronized."
                );

            } else if (
                    sameReplicationHistory
                            &&
                    replicaState.sequence >= 0
                            &&
                    masterSequence
                            >
                    replicaState.sequence
                            &&
                    ReplicationBacklog
                            .canServeFrom(
                                    replicaState.sequence
                            )
            ) {

                performPartialSync(
                        replicaState.sequence
                );

            } else {

                if (!sameReplicationHistory) {

                    System.out.println(
                            "Replication history mismatch."
                    );

                    System.out.println(
                            "FULL SYNC required."
                    );

                } else if (
                        replicaState.sequence
                                >
                        masterSequence
                ) {

                    System.out.println(
                            "Replica sequence is ahead of master."
                    );

                    System.out.println(
                            "FULL SYNC required."
                    );

                } else {

                    System.out.println(
                            "Required commands are not available in backlog."
                    );

                    System.out.println(
                            "FULL SYNC required."
                    );
                }

                performFullSync();
            }

            target.setConnected(
                    true
            );

            System.out.println(
                    "Replica ONLINE: "
                            + target.getId()
            );
        }

        // =====================================================
        // REPLGETSTATE
        // =====================================================

        private ReplicaState requestReplicaState()
                throws IOException {

            sendArguments(
                    Collections.singletonList(
                            "REPLGETSTATE"
                    )
            );

            List<String> state =
                    readStringArrayResponse();

            if (
                    state.size()
                            !=
                    3
            ) {

                throw new IOException(
                        "Invalid REPLGETSTATE response"
                );
            }

            String replicationId =
                    state.get(0);

            if (
                    replicationId == null
                            ||
                    replicationId.isBlank()
                            ||
                    replicationId.equalsIgnoreCase(
                            "none"
                    )
            ) {

                replicationId =
                        null;
            }

            long epoch;

            long sequence;

            try {

                epoch =
                        Long.parseLong(
                                state.get(1)
                        );

                sequence =
                        Long.parseLong(
                                state.get(2)
                        );

            } catch (
                    NumberFormatException e
            ) {

                throw new IOException(
                        "Invalid REPLGETSTATE numeric values"
                );
            }

            return new ReplicaState(
                    replicationId,
                    epoch,
                    sequence
            );
        }

        // =====================================================
        // PARTIAL SYNC
        // =====================================================

        private void performPartialSync(
                long replicaSequence
        ) throws IOException {

            List<ReplicationBacklog.Entry> entries =
                    ReplicationBacklog
                            .getAfter(
                                    replicaSequence
                            );

            if (entries.isEmpty()) {

                performFullSync();

                return;
            }

            long startSequence =
                    entries
                            .get(0)
                            .getSequence();

            long endSequence =
                    entries
                            .get(
                                    entries.size() - 1
                            )
                            .getSequence();

            String replicationId =
                    ReplicationMetadata
                            .getMasterReplicationId();

            long epoch =
                    ReplicationMetadata
                            .getMasterEpoch();

            System.out.println(
                    "Starting PARTIAL SYNC -> "
                            + target.getId()
            );

            System.out.println(
                    "REPL ID = "
                            + ReplicationId.shortId(
                                    replicationId
                            )
                            + "..."
            );

            System.out.println(
                    "Epoch = "
                            + epoch
            );

            System.out.println(
                    "Sequences "
                            + startSequence
                            + " -> "
                            + endSequence
            );

            System.out.println(
                    "Missing commands = "
                            + entries.size()
            );

            // ================================================
            // BEGIN
            // ================================================

            sendArguments(
                    Arrays.asList(
                            "REPLPARTIALSYNCBEGIN",
                            replicationId,
                            String.valueOf(
                                    epoch
                            ),
                            String.valueOf(
                                    startSequence
                            ),
                            String.valueOf(
                                    endSequence
                            )
                    )
            );

            String begin =
                    readSimpleResponse();

            if (
                    !"OK".equalsIgnoreCase(
                            begin
                    )
            ) {

                throw new IOException(
                        "Replica rejected partial sync begin"
                );
            }

            // ================================================
            // MISSED COMMANDS
            // ================================================

            for (
                    ReplicationBacklog.Entry entry :
                    entries
            ) {

                sendReplicationCommand(
                        entry.getSequence(),
                        entry.getCommand()
                );

                boolean accepted =
                        readCommandResponse();

                if (!accepted) {

                    throw new IOException(
                            "Replica rejected partial sync sequence "
                                    + entry.getSequence()
                    );
                }

                target.incrementCommandsSent();
            }

            // ================================================
            // END
            // ================================================

            sendArguments(
                    Arrays.asList(
                            "REPLPARTIALSYNCEND",
                            replicationId,
                            String.valueOf(
                                    epoch
                            ),
                            String.valueOf(
                                    endSequence
                            )
                    )
            );

            String end =
                    readSimpleResponse();

            if (
                    !"OK".equalsIgnoreCase(
                            end
                    )
            ) {

                throw new IOException(
                        "Replica rejected partial sync end"
                );
            }

            target.setLastSyncedSequence(
                    endSequence
            );

            target.incrementPartialSyncCount();

            System.out.println(
                    "PARTIAL SYNC completed -> "
                            + target.getId()
            );
        }

        // =====================================================
        // FULL SYNC
        // =====================================================

        private void performFullSync()
                throws IOException {

            FullSyncProvider provider =
                    fullSyncProvider;

            if (provider == null) {

                throw new IOException(
                        "FullSyncProvider not configured"
                );
            }

            FullSyncPayload payload =
                    provider.createPayload();

            if (payload == null) {

                throw new IOException(
                        "Full sync payload is null"
                );
            }

            long syncSequence =
                    payload.getSequence();

            String replicationId =
                    ReplicationMetadata
                            .getMasterReplicationId();

            long epoch =
                    ReplicationMetadata
                            .getMasterEpoch();

            if (
                    !ReplicationId.isValid(
                            replicationId
                    )
            ) {

                throw new IOException(
                        "Master replication ID is invalid"
                );
            }

            System.out.println(
                    "Starting FULL SYNC -> "
                            + target.getId()
            );

            System.out.println(
                    "REPL ID = "
                            + ReplicationId.shortId(
                                    replicationId
                            )
                            + "..."
            );

            System.out.println(
                    "Epoch = "
                            + epoch
            );

            System.out.println(
                    "Sequence = "
                            + syncSequence
            );

            System.out.println(
                    "Commands = "
                            + payload.size()
            );

            // ================================================
            // BEGIN
            // ================================================

            sendArguments(
                    Arrays.asList(
                            "REPLFULLSYNCBEGIN",
                            replicationId,
                            String.valueOf(
                                    epoch
                            ),
                            String.valueOf(
                                    syncSequence
                            )
                    )
            );

            String begin =
                    readSimpleResponse();

            if (
                    !"OK".equalsIgnoreCase(
                            begin
                    )
            ) {

                throw new IOException(
                        "Replica rejected full sync begin"
                );
            }

            // ================================================
            // DATABASE STATE
            // ================================================

            for (
                    String command :
                    payload.getCommands()
            ) {

                sendCommand(
                        command
                );

                boolean accepted =
                        readCommandResponse();

                if (!accepted) {

                    throw new IOException(
                            "Replica rejected full sync command: "
                                    + command
                    );
                }
            }

            // ================================================
            // END
            // ================================================

            sendArguments(
                    Arrays.asList(
                            "REPLFULLSYNCEND",
                            replicationId,
                            String.valueOf(
                                    epoch
                            ),
                            String.valueOf(
                                    syncSequence
                            )
                    )
            );

            String end =
                    readSimpleResponse();

            if (
                    !"OK".equalsIgnoreCase(
                            end
                    )
            ) {

                throw new IOException(
                        "Replica rejected full sync end"
                );
            }

            target.setLastSyncedSequence(
                    syncSequence
            );

            target.incrementFullSyncCount();

            System.out.println(
                    "FULL SYNC completed -> "
                            + target.getId()
            );
        }

        // =====================================================
        // SEND REPLICATED COMMAND
        // =====================================================

        private void sendReplicationCommand(
                long sequence,
                String command
        ) throws IOException {

            List<String> arguments =
                    new ArrayList<>();

            arguments.add(
                    "REPLCMD"
            );

            arguments.add(
                    String.valueOf(
                            sequence
                    )
            );

            String[] commandArguments =
                    command
                            .trim()
                            .split(
                                    "\\s+"
                            );

            arguments.addAll(
                    Arrays.asList(
                            commandArguments
                    )
            );

            sendArguments(
                    arguments
            );
        }

        // =====================================================
        // SEND NORMAL COMMAND
        // =====================================================

        private void sendCommand(
                String command
        ) throws IOException {

            String[] arguments =
                    command
                            .trim()
                            .split(
                                    "\\s+"
                            );

            sendArguments(
                    Arrays.asList(
                            arguments
                    )
            );
        }

        // =====================================================
        // SEND RESP
        // =====================================================

        private void sendArguments(
                List<String> arguments
        ) throws IOException {

            if (output == null) {

                throw new IOException(
                        "Replica output stream unavailable"
                );
            }

            ByteArrayOutputStream buffer =
                    new ByteArrayOutputStream();

            buffer.write(
                    (
                            "*"
                                    + arguments.size()
                                    + "\r\n"
                    ).getBytes(
                            StandardCharsets.UTF_8
                    )
            );

            for (
                    String argument :
                    arguments
            ) {

                if (argument == null) {

                    argument =
                            "";
                }

                byte[] data =
                        argument.getBytes(
                                StandardCharsets.UTF_8
                        );

                buffer.write(
                        (
                                "$"
                                        + data.length
                                        + "\r\n"
                        ).getBytes(
                                StandardCharsets.UTF_8
                        )
                );

                buffer.write(
                        data
                );

                buffer.write(
                        "\r\n".getBytes(
                                StandardCharsets.UTF_8
                        )
                );
            }

            output.write(
                    buffer.toByteArray()
            );

            output.flush();
        }

        // =====================================================
        // READ ARRAY RESPONSE
        // =====================================================

        private List<String> readStringArrayResponse()
                throws IOException {

            if (input == null) {

                throw new IOException(
                        "Replica input stream unavailable"
                );
            }

            int type =
                    input.read();

            if (type == -1) {

                throw new EOFException(
                        "Replica disconnected"
                );
            }

            if (type == '-') {

                throw new IOException(
                        readLine(
                                input
                        )
                );
            }

            if (type != '*') {

                throw new IOException(
                        "Expected array response but received "
                                + (char) type
                );
            }

            int count =
                    Integer.parseInt(
                            readLine(
                                    input
                            )
                    );

            if (count < 0) {

                return Collections.emptyList();
            }

            List<String> result =
                    new ArrayList<>(
                            count
                    );

            for (
                    int i = 0;
                    i < count;
                    i++
            ) {

                result.add(
                        readScalarAsString()
                );
            }

            return result;
        }

        // =====================================================
        // READ SCALAR AS STRING
        // =====================================================

        private String readScalarAsString()
                throws IOException {

            int type =
                    input.read();

            if (type == -1) {

                throw new EOFException(
                        "Replica disconnected"
                );
            }

            if (type == '-') {

                throw new IOException(
                        readLine(
                                input
                        )
                );
            }

            if (
                    type == '+'
                            ||
                    type == ':'
            ) {

                return readLine(
                        input
                );
            }

            if (type == '$') {

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
                        readExactlyBytes(
                                input,
                                length
                        );

                readCrLf();

                return new String(
                        data,
                        StandardCharsets.UTF_8
                );
            }

            throw new IOException(
                    "Unsupported scalar RESP type: "
                            + (char) type
            );
        }

        // =====================================================
        // READ SIMPLE RESPONSE
        // =====================================================

        private String readSimpleResponse()
                throws IOException {

            if (input == null) {

                throw new IOException(
                        "Replica input stream unavailable"
                );
            }

            int type =
                    input.read();

            if (type == -1) {

                throw new EOFException(
                        "Replica disconnected"
                );
            }

            String line =
                    readLine(
                            input
                    );

            if (type == '-') {

                throw new IOException(
                        line
                );
            }

            if (type != '+') {

                throw new IOException(
                        "Expected simple response but received "
                                + (char) type
                );
            }

            return line;
        }

        // =====================================================
        // READ GENERIC COMMAND RESPONSE
        // =====================================================

        private boolean readCommandResponse()
                throws IOException {

            if (input == null) {

                throw new IOException(
                        "Replica input stream unavailable"
                );
            }

            int type =
                    input.read();

            if (type == -1) {

                throw new EOFException(
                        "Replica disconnected"
                );
            }

            switch (type) {

                case '-': {

                    String error =
                            readLine(
                                    input
                            );

                    System.out.println(
                            target.getId()
                                    + " error: "
                                    + error
                    );

                    return false;
                }

                case '+':
                case ':': {

                    readLine(
                            input
                    );

                    return true;
                }

                case '$': {

                    int length =
                            Integer.parseInt(
                                    readLine(
                                            input
                                    )
                            );

                    if (length >= 0) {

                        readExactlyBytes(
                                input,
                                length
                        );

                        readCrLf();
                    }

                    return true;
                }

                case '*': {

                    int count =
                            Integer.parseInt(
                                    readLine(
                                            input
                                    )
                            );

                    if (count < 0) {

                        return true;
                    }

                    for (
                            int i = 0;
                            i < count;
                            i++
                    ) {

                        if (
                                !readCommandResponse()
                        ) {

                            return false;
                        }
                    }

                    return true;
                }

                default:

                    throw new IOException(
                            "Unknown RESP response type: "
                                    + (char) type
                    );
            }
        }

        // =====================================================
        // READ LINE
        // =====================================================

        private String readLine(
                InputStream stream
        ) throws IOException {

            ByteArrayOutputStream buffer =
                    new ByteArrayOutputStream();

            while (true) {

                int b =
                        stream.read();

                if (b == -1) {

                    throw new EOFException(
                            "Unexpected end of stream"
                    );
                }

                if (b == '\n') {

                    break;
                }

                if (b != '\r') {

                    buffer.write(
                            b
                    );
                }
            }

            return buffer.toString(
                    StandardCharsets.UTF_8
            );
        }

        // =====================================================
        // EXACT BYTES
        // =====================================================

        private byte[] readExactlyBytes(
                InputStream stream,
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
                        stream.read(
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

        // =====================================================
        // CRLF
        // =====================================================

        private void readCrLf()
                throws IOException {

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

        // =====================================================
        // CLOSE TRANSPORT
        // =====================================================

        private synchronized void closeTransportOnly() {

            try {

                if (socket != null) {

                    socket.close();
                }

            } catch (
                    IOException ignored
            ) {
            }

            socket =
                    null;

            input =
                    null;

            output =
                    null;
        }

        // =====================================================
        // STOP
        // =====================================================

        private void stop() {

            workerRunning =
                    false;

            closeTransportOnly();

            target.setConnected(
                    false
            );

            if (thread != null) {

                thread.interrupt();
            }

            queue.clear();
        }

        private int getQueueSize() {

            return queue.size();
        }
    }

    // =========================================================
    // FULL SYNC PROVIDER
    // =========================================================

    public static void setFullSyncProvider(
            FullSyncProvider provider
    ) {

        fullSyncProvider =
                provider;
    }

    // =========================================================
    // SINGLE REPLICA
    // =========================================================

    public static synchronized void startMaster(
            String host,
            int port
    ) {

        List<ReplicaTarget> targets =
                new ArrayList<>();

        targets.add(
                new ReplicaTarget(
                        host,
                        port
                )
        );

        startMaster(
                targets
        );
    }

    // =========================================================
    // START MASTER
    // =========================================================

    public static synchronized void startMaster(
            List<ReplicaTarget> targets
    ) {

        if (running) {

            System.out.println(
                    "Replication manager already running."
            );

            return;
        }

        /*
         * Restore persisted replication sequence.
         *
         * On a MASTER this value represents the current
         * replication offset.
         *
         * On a promoted REPLICA it already contains its
         * latest applied upstream offset.
         */
        long persistedSequence =
                ReplicationMetadata
                        .getReplicaSequence();

        if (persistedSequence < 0) {

            persistedSequence =
                    0;
        }

        replicationSequence.set(
                persistedSequence
        );

        enabled =
                true;

        running =
                true;

        System.out.println(
                "Multi-Replica Replication = enabled"
        );

        System.out.println(
                "Replication ID Safety = enabled"
        );

        System.out.println(
                "Master Replication ID = "
                        +
                        ReplicationId.shortId(
                                ReplicationMetadata
                                        .getMasterReplicationId()
                        )
                        + "..."
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
                        replicationSequence.get()
        );

        System.out.println(
                "Heartbeat Monitoring = enabled"
        );

        System.out.println(
                "Heartbeat Interval = "
                        + HEARTBEAT_INTERVAL_MS
                        + " ms"
        );

        System.out.println(
                "Configured replicas = "
                        +
                        (
                                targets == null
                                        ?
                                0
                                        :
                                targets.size()
                        )
        );

        System.out.println(
                "Replication Backlog = "
                        +
                        ReplicationBacklog
                                .getMaxBacklogSize()
                        +
                        " commands"
        );

        if (targets == null) {

            return;
        }

        for (
                ReplicaTarget target :
                targets
        ) {

            addReplica(
                    target
            );
        }
    }

    // =========================================================
    // ADD REPLICA
    // =========================================================

    public static synchronized boolean addReplica(
            ReplicaTarget target
    ) {

        if (target == null) {

            return false;
        }

        String id =
                target.getId();

        if (
                workers.containsKey(
                        id
                )
        ) {

            System.out.println(
                    "Replica already configured: "
                            + id
            );

            return false;
        }

        ReplicaWorker worker =
                new ReplicaWorker(
                        target
                );

        workers.put(
                id,
                worker
        );

        worker.start();

        System.out.println(
                "Replica added: "
                        + id
        );

        return true;
    }

    // =========================================================
    // REMOVE REPLICA
    // =========================================================

    public static synchronized boolean removeReplica(
            String host,
            int port
    ) {

        String id =
                host
                        + ":"
                        + port;

        ReplicaWorker worker =
                workers.remove(
                        id
                );

        if (worker == null) {

            return false;
        }

        worker.stop();

        System.out.println(
                "Replica removed: "
                        + id
        );

        return true;
    }

    // =========================================================
    // PROPAGATE WRITE
    // =========================================================

    public static long propagate(
            String command
    ) {

        if (
                !enabled
                        ||
                command == null
                        ||
                command.isBlank()
        ) {

            return -1;
        }

        long sequence =
                replicationSequence
                        .incrementAndGet();

        /*
         * Persist master offset so restart does not
         * reset sequence to zero while keeping same
         * replication ID.
         */
        ReplicationMetadata
                .setReplicaSequence(
                        sequence
                );

        ReplicationBacklog.add(
                sequence,
                command
        );

        ReplicationCommand item =
                new ReplicationCommand(
                        sequence,
                        command
                );

        for (
                ReplicaWorker worker :
                workers.values()
        ) {

            worker.enqueue(
                    item
            );
        }

        return sequence;
    }

    // =========================================================
    // SEQUENCE INITIALIZER
    // =========================================================

    public static synchronized void initializeSequence(
            long sequence
    ) {

        if (running) {

            throw new IllegalStateException(
                    "Cannot initialize sequence while replication is running"
            );
        }

        long safe =
                Math.max(
                        sequence,
                        0
                );

        replicationSequence.set(
                safe
        );

        ReplicationMetadata
                .setReplicaSequence(
                        safe
                );
    }

    // =========================================================
    // STOP
    // =========================================================

    public static synchronized void stop() {

        running =
                false;

        enabled =
                false;

        for (
                ReplicaWorker worker :
                workers.values()
        ) {

            worker.stop();
        }

        workers.clear();

        ReplicationBacklog.clear();

        System.out.println(
                "Replication manager stopped."
        );
    }

    // =========================================================
    // BASIC STATUS
    // =========================================================

    public static boolean isEnabled() {

        return enabled;
    }

    public static boolean isConnected() {

        return getConnectedReplicaCount()
                >
                0;
    }

    public static int getReplicaCount() {

        return workers.size();
    }

    public static int getConnectedReplicaCount() {

        int count =
                0;

        for (
                ReplicaWorker worker :
                workers.values()
        ) {

            if (
                    worker.target
                            .isConnected()
            ) {

                count++;
            }
        }

        return count;
    }

    // =========================================================
    // HEALTH COUNTS
    // =========================================================

    public static int getHealthyReplicaCount() {

        int count =
                0;

        for (
                ReplicaWorker worker :
                workers.values()
        ) {

            if (
                    worker.target
                            .getHealthStatus()
                            ==
                    ReplicaHealth.Status.ONLINE
            ) {

                count++;
            }
        }

        return count;
    }

    public static int getSuspectReplicaCount() {

        int count =
                0;

        for (
                ReplicaWorker worker :
                workers.values()
        ) {

            if (
                    worker.target
                            .getHealthStatus()
                            ==
                    ReplicaHealth.Status.SUSPECT
            ) {

                count++;
            }
        }

        return count;
    }

    public static int getOfflineReplicaCount() {

        int count =
                0;

        for (
                ReplicaWorker worker :
                workers.values()
        ) {

            if (
                    worker.target
                            .getHealthStatus()
                            ==
                    ReplicaHealth.Status.OFFLINE
            ) {

                count++;
            }
        }

        return count;
    }

    // =========================================================
    // CURRENT SEQUENCE
    // =========================================================

    public static long getCurrentSequence() {

        return replicationSequence.get();
    }

    public static long getLastSyncedSequence() {

        if (workers.isEmpty()) {

            return 0;
        }

        long minimum =
                Long.MAX_VALUE;

        for (
                ReplicaWorker worker :
                workers.values()
        ) {

            minimum =
                    Math.min(
                            minimum,
                            worker.target
                                    .getLastSyncedSequence()
                    );
        }

        return minimum
                ==
                Long.MAX_VALUE
                ?
                0
                :
                minimum;
    }

    public static long getLastReplicaReportedSequence() {

        if (workers.isEmpty()) {

            return 0;
        }

        long minimum =
                Long.MAX_VALUE;

        for (
                ReplicaWorker worker :
                workers.values()
        ) {

            minimum =
                    Math.min(
                            minimum,
                            worker.target
                                    .getLastReportedSequence()
                    );
        }

        return minimum
                ==
                Long.MAX_VALUE
                ?
                0
                :
                minimum;
    }

    // =========================================================
    // STATISTICS
    // =========================================================

    public static long getCommandsSent() {

        long total =
                0;

        for (
                ReplicaWorker worker :
                workers.values()
        ) {

            total +=
                    worker.target
                            .getCommandsSent();
        }

        return total;
    }

    public static long getFailedCommands() {

        long total =
                0;

        for (
                ReplicaWorker worker :
                workers.values()
        ) {

            total +=
                    worker.target
                            .getFailedCommands();
        }

        return total;
    }

    public static long getReconnects() {

        long total =
                0;

        for (
                ReplicaWorker worker :
                workers.values()
        ) {

            total +=
                    worker.target
                            .getReconnects();
        }

        return total;
    }

    public static long getFullSyncCount() {

        long total =
                0;

        for (
                ReplicaWorker worker :
                workers.values()
        ) {

            total +=
                    worker.target
                            .getFullSyncCount();
        }

        return total;
    }

    public static long getPartialSyncCount() {

        long total =
                0;

        for (
                ReplicaWorker worker :
                workers.values()
        ) {

            total +=
                    worker.target
                            .getPartialSyncCount();
        }

        return total;
    }

    public static long getSkippedCommands() {

        return skippedCommands.get();
    }

    public static int getQueueSize() {

        int total =
                0;

        for (
                ReplicaWorker worker :
                workers.values()
        ) {

            total +=
                    worker.getQueueSize();
        }

        return total;
    }

    // =========================================================
    // HEARTBEAT LATENCY
    // =========================================================

    public static long getAverageHeartbeatLatencyMillis() {

        long total =
                0;

        int count =
                0;

        for (
                ReplicaWorker worker :
                workers.values()
        ) {

            long latency =
                    worker.target
                            .getHeartbeatLatencyMillis();

            if (latency >= 0) {

                total +=
                        latency;

                count++;
            }
        }

        if (count == 0) {

            return -1;
        }

        return total
                /
                count;
    }

    // =========================================================
    // TARGETS
    // =========================================================

    public static List<ReplicaTarget> getReplicaTargets() {

        List<ReplicaTarget> result =
                new ArrayList<>();

        for (
                ReplicaWorker worker :
                workers.values()
        ) {

            result.add(
                    worker.target
            );
        }

        result.sort(
                Comparator.comparingInt(
                        ReplicaTarget::getPort
                )
        );

        return result;
    }

    // =========================================================
    // STATUS
    // =========================================================

    public static String getStatus() {

        StringBuilder result =
                new StringBuilder();

        String replicationId =
                ReplicationMetadata
                        .getMasterReplicationId();

        result.append(
                "replication_enabled:"
        );

        result.append(
                enabled
        );

        result.append(
                "\nmaster_repl_id:"
        );

        result.append(
                replicationId == null
                        ?
                        "none"
                        :
                        replicationId
        );

        result.append(
                "\nmaster_epoch:"
        );

        result.append(
                ReplicationMetadata
                        .getMasterEpoch()
        );

        result.append(
                "\nmaster_sequence:"
        );

        result.append(
                replicationSequence.get()
        );

        result.append(
                "\nconfigured_replicas:"
        );

        result.append(
                getReplicaCount()
        );

        result.append(
                "\nconnected_replicas:"
        );

        result.append(
                getConnectedReplicaCount()
        );

        result.append(
                "\nhealthy_replicas:"
        );

        result.append(
                getHealthyReplicaCount()
        );

        result.append(
                "\nsuspect_replicas:"
        );

        result.append(
                getSuspectReplicaCount()
        );

        result.append(
                "\noffline_replicas:"
        );

        result.append(
                getOfflineReplicaCount()
        );

        result.append(
                "\nheartbeat_interval_ms:"
        );

        result.append(
                HEARTBEAT_INTERVAL_MS
        );

        result.append(
                "\naverage_heartbeat_latency_ms:"
        );

        result.append(
                getAverageHeartbeatLatencyMillis()
        );

        result.append(
                "\nbacklog_size:"
        );

        result.append(
                ReplicationBacklog.size()
        );

        result.append(
                "\nbacklog_oldest_sequence:"
        );

        result.append(
                ReplicationBacklog
                        .getOldestSequence()
        );

        result.append(
                "\nbacklog_newest_sequence:"
        );

        result.append(
                ReplicationBacklog
                        .getNewestSequence()
        );

        result.append(
                "\nskipped_commands:"
        );

        result.append(
                skippedCommands.get()
        );

        int number =
                1;

        for (
                ReplicaTarget target :
                getReplicaTargets()
        ) {

            result.append(
                    "\n\nreplica_"
            );

            result.append(
                    number
            );

            result.append(
                    ":"
            );

            result.append(
                    target.getId()
            );

            result.append(
                    "\nreplica_"
            );

            result.append(
                    number
            );

            result.append(
                    "_connected:"
            );

            result.append(
                    target.isConnected()
            );

            result.append(
                    "\nreplica_"
            );

            result.append(
                    number
            );

            result.append(
                    "_health:"
            );

            result.append(
                    target.getHealthStatus()
            );

            result.append(
                    "\nreplica_"
            );

            result.append(
                    number
            );

            result.append(
                    "_heartbeat_latency_ms:"
            );

            result.append(
                    target.getHeartbeatLatencyMillis()
            );

            result.append(
                    "\nreplica_"
            );

            result.append(
                    number
            );

            result.append(
                    "_heartbeat_failures:"
            );

            result.append(
                    target
                            .getConsecutiveHeartbeatFailures()
            );

            result.append(
                    "\nreplica_"
            );

            result.append(
                    number
            );

            result.append(
                    "_successful_heartbeats:"
            );

            result.append(
                    target
                            .getSuccessfulHeartbeats()
            );

            result.append(
                    "\nreplica_"
            );

            result.append(
                    number
            );

            result.append(
                    "_failed_heartbeats:"
            );

            result.append(
                    target
                            .getFailedHeartbeats()
            );

            result.append(
                    "\nreplica_"
            );

            result.append(
                    number
            );

            result.append(
                    "_reported_sequence:"
            );

            result.append(
                    target
                            .getLastReportedSequence()
            );

            result.append(
                    "\nreplica_"
            );

            result.append(
                    number
            );

            result.append(
                    "_synced_sequence:"
            );

            result.append(
                    target
                            .getLastSyncedSequence()
            );

            result.append(
                    "\nreplica_"
            );

            result.append(
                    number
            );

            result.append(
                    "_full_syncs:"
            );

            result.append(
                    target
                            .getFullSyncCount()
            );

            result.append(
                    "\nreplica_"
            );

            result.append(
                    number
            );

            result.append(
                    "_partial_syncs:"
            );

            result.append(
                    target
                            .getPartialSyncCount()
            );

            result.append(
                    "\nreplica_"
            );

            result.append(
                    number
            );

            result.append(
                    "_reconnects:"
            );

            result.append(
                    target.getReconnects()
            );

            number++;
        }

        return result.toString();
    }
}