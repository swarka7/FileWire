package filewire.server;

import filewire.protocol.ErrorCode;
import filewire.protocol.Frame;
import filewire.protocol.FrameCodec;
import filewire.protocol.MessageType;
import filewire.protocol.PayloadCodec;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Bounded concurrent TCP server for the FileWire protocol. */
public final class FileWireServer implements AutoCloseable {
    private static final System.Logger LOGGER = System.getLogger(FileWireServer.class.getName());

    private final ServerConfig config;
    private final TransferService transfers;
    private final SessionRegistry sessions = new SessionRegistry();
    private final ThreadPoolExecutor workers;
    private final Semaphore connectionSlots;
    private final CountDownLatch stopped = new CountDownLatch(1);
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    private volatile ServerSocket serverSocket;
    private volatile Thread acceptorThread;

    public FileWireServer(ServerConfig config) throws IOException {
        this.config = Objects.requireNonNull(config, "config");
        transfers = new TransferService(new StorageService(config.storageRoot(), config.maxFileSizeBytes()));
        connectionSlots = new Semaphore(config.maximumConnections(), true);
        workers = new ThreadPoolExecutor(
                config.workerThreads(),
                config.workerThreads(),
                0,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(config.queueCapacity()),
                namedThreadFactory("filewire-worker-"),
                new ThreadPoolExecutor.AbortPolicy());
    }

    /** Binds the listening socket before returning, then accepts clients on a dedicated thread. */
    public synchronized void start() throws IOException {
        if (closed.get()) {
            throw new IllegalStateException("Server is closed");
        }
        if (running.get()) {
            throw new IllegalStateException("Server is already running");
        }

        ServerSocket listener = new ServerSocket();
        boolean bound = false;
        try {
            listener.setReuseAddress(true);
            listener.bind(new InetSocketAddress(config.port()));
            bound = true;
            serverSocket = listener;
            running.set(true);
            Thread acceptor = new Thread(this::acceptLoop, "filewire-acceptor");
            acceptorThread = acceptor;
            acceptor.start();
            LOGGER.log(
                    System.Logger.Level.INFO,
                    "FileWire server listening on port {0}, storage {1}",
                    listener.getLocalPort(),
                    config.storageRoot());
        } finally {
            if (!bound) {
                listener.close();
            }
        }
    }

    /** Returns the assigned local port after {@link #start()}, including for configured port 0. */
    public int port() {
        ServerSocket listener = serverSocket;
        if (listener == null || !listener.isBound()) {
            throw new IllegalStateException("Server has not started");
        }
        return listener.getLocalPort();
    }

    public boolean isRunning() {
        return running.get() && !closed.get();
    }

    public int activeSessionCount() {
        return sessions.size();
    }

    public int maximumConnections() {
        return config.maximumConnections();
    }

    /** Waits until the accept loop ends or the server is closed. */
    public void awaitTermination() throws InterruptedException {
        stopped.await();
    }

    @Override
    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        running.set(false);
        closeListener();
        joinAcceptor(config.shutdownTimeout());

        for (ClientSession session : sessions.snapshot()) {
            session.close();
        }

        workers.shutdown();
        boolean terminated = awaitWorkers(config.shutdownTimeout());
        if (!terminated) {
            workers.shutdownNow();
            for (ClientSession session : sessions.snapshot()) {
                session.close();
            }
            awaitWorkers(config.shutdownTimeout());
        }

        try {
            transfers.close();
        } catch (IOException exception) {
            LOGGER.log(System.Logger.Level.WARNING, "Could not fully clean server transfer storage", exception);
        } finally {
            stopped.countDown();
        }
        LOGGER.log(System.Logger.Level.INFO, "FileWire server stopped");
    }

    private void acceptLoop() {
        try {
            while (running.get()) {
                Socket socket = serverSocket.accept();
                if (!running.get()) {
                    closeSocket(socket);
                    break;
                }
                try {
                    configure(socket);
                    admit(socket);
                } catch (SocketException exception) {
                    closeSocket(socket);
                    if (running.get()) {
                        LOGGER.log(
                                System.Logger.Level.WARNING,
                                "Could not configure an accepted client socket",
                                exception);
                    }
                }
            }
        } catch (SocketException exception) {
            if (running.get()) {
                LOGGER.log(System.Logger.Level.ERROR, "Server socket failed", exception);
            }
        } catch (IOException exception) {
            if (running.get()) {
                LOGGER.log(System.Logger.Level.ERROR, "Failed while accepting a client", exception);
            }
        } finally {
            running.set(false);
            stopped.countDown();
        }
    }

    private void admit(Socket socket) {
        if (!connectionSlots.tryAcquire()) {
            rejectBusy(socket);
            return;
        }

        long sessionId;
        try {
            sessionId = sessions.nextSessionId();
        } catch (RuntimeException exception) {
            connectionSlots.release();
            rejectBusy(socket);
            LOGGER.log(System.Logger.Level.ERROR, "Could not allocate a session ID", exception);
            return;
        }

        AtomicReference<ClientSession> sessionReference = new AtomicReference<>();
        try {
            ClientSession session = new ClientSession(
                    sessionId,
                    socket,
                    transfers,
                    () -> {
                        ClientSession closedSession = sessionReference.get();
                        if (closedSession != null) {
                            sessions.unregister(sessionId, closedSession);
                        }
                        connectionSlots.release();
                    });
            sessionReference.set(session);
            sessions.register(sessionId, session);
            try {
                workers.execute(session);
            } catch (RejectedExecutionException exception) {
                LOGGER.log(System.Logger.Level.WARNING, "Worker queue rejected session {0}", sessionId);
                session.rejectBusy();
            }
        } catch (IOException | RuntimeException exception) {
            connectionSlots.release();
            closeSocket(socket);
            LOGGER.log(System.Logger.Level.WARNING, "Could not initialize session " + sessionId, exception);
        }
    }

    private void rejectBusy(Socket socket) {
        try (Socket rejected = socket;
             BufferedOutputStream output = new BufferedOutputStream(rejected.getOutputStream())) {
            Frame frame = new Frame(
                    MessageType.ERROR,
                    1,
                    PayloadCodec.encodeError(new PayloadCodec.Error(
                            ErrorCode.SERVER_BUSY,
                            "Server connection limit reached")));
            FrameCodec.write(output, frame);
            output.flush();
        } catch (IOException ignored) {
            // Best effort: closing the socket still gives the peer a deterministic rejection.
        }
        LOGGER.log(System.Logger.Level.WARNING, "Rejected connection because the server is at capacity");
    }

    private void configure(Socket socket) throws SocketException {
        socket.setSoTimeout(config.socketReadTimeoutMillis());
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);
    }

    private void closeListener() {
        ServerSocket listener = serverSocket;
        if (listener != null) {
            try {
                listener.close();
            } catch (IOException exception) {
                LOGGER.log(System.Logger.Level.DEBUG, "Could not close server socket", exception);
            }
        }
    }

    private void joinAcceptor(Duration timeout) {
        Thread acceptor = acceptorThread;
        if (acceptor == null || acceptor == Thread.currentThread()) {
            return;
        }
        try {
            acceptor.join(timeout.toMillis());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean awaitWorkers(Duration timeout) {
        try {
            return workers.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static ThreadFactory namedThreadFactory(String prefix) {
        AtomicInteger number = new AtomicInteger(1);
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + number.getAndIncrement());
            thread.setDaemon(false);
            return thread;
        };
    }

    private static void closeSocket(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // Socket is already unusable.
        }
    }
}
