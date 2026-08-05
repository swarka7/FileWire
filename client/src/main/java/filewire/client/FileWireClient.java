package filewire.client;

import filewire.protocol.DigestUtil;
import filewire.protocol.ErrorCode;
import filewire.protocol.FilenameValidator;
import filewire.protocol.Frame;
import filewire.protocol.FrameCodec;
import filewire.protocol.MessageType;
import filewire.protocol.PayloadCodec;
import filewire.protocol.PayloadCodec.DeleteRequest;
import filewire.protocol.PayloadCodec.DownloadMetadata;
import filewire.protocol.PayloadCodec.DownloadRequest;
import filewire.protocol.PayloadCodec.FileChunk;
import filewire.protocol.PayloadCodec.TransferComplete;
import filewire.protocol.PayloadCodec.UploadAccepted;
import filewire.protocol.PayloadCodec.UploadRequest;
import filewire.protocol.ProtocolConstants;
import filewire.protocol.ProtocolException;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/** Synchronous, one-operation-at-a-time client for the FileWire protocol. */
public final class FileWireClient implements AutoCloseable {
    private static final int CONNECT_TIMEOUT_MILLIS = 5_000;
    private static final int READ_TIMEOUT_MILLIS = 60_000;
    private static final long DEFAULT_MAX_DOWNLOAD_BYTES = 16L * 1024 * 1024 * 1024;
    private static final long PREPARATION_KEEPALIVE_NANOS = Duration.ofMillis(
            ProtocolConstants.PREPARATION_KEEPALIVE_INTERVAL_MILLIS).toNanos();
    private static final Path DEFAULT_DOWNLOAD_ROOT = Path.of("downloads");

    private final Socket socket;
    private final InputStream input;
    private final OutputStream output;
    private final Path downloadRoot;
    private final long maximumDownloadSize;
    private final AtomicLong requestIds = new AtomicLong(1);
    private final ReentrantLock operationLock = new ReentrantLock();
    private volatile boolean connected = true;

    private FileWireClient(
            Socket socket,
            Path downloadRoot,
            long maximumDownloadSize) throws IOException {
        this.socket = socket;
        this.downloadRoot = downloadRoot;
        this.maximumDownloadSize = maximumDownloadSize;
        this.input = new BufferedInputStream(socket.getInputStream());
        this.output = new BufferedOutputStream(socket.getOutputStream());
    }

    public static FileWireClient connect(String host, int port) throws IOException {
        return connect(host, port, DEFAULT_DOWNLOAD_ROOT, DEFAULT_MAX_DOWNLOAD_BYTES);
    }

    public static FileWireClient connect(String host, int port, Path downloadRoot) throws IOException {
        return connect(host, port, downloadRoot, DEFAULT_MAX_DOWNLOAD_BYTES);
    }

    public static FileWireClient connect(
            String host,
            int port,
            Path downloadRoot,
            long maximumDownloadSize) throws IOException {
        Objects.requireNonNull(host, "host");
        if (host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
        if (maximumDownloadSize < 0) {
            throw new IllegalArgumentException("maximumDownloadSize must not be negative");
        }

        Path preparedRoot = AtomicDownload.prepareRoot(downloadRoot);
        Socket socket = new Socket();
        FileWireClient client = null;
        try {
            socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MILLIS);
            socket.setSoTimeout(READ_TIMEOUT_MILLIS);
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);
            client = new FileWireClient(socket, preparedRoot, maximumDownloadSize);
            client.handshake();
            return client;
        } catch (IOException | RuntimeException exception) {
            if (client != null) {
                client.abortConnection(exception);
            } else {
                try {
                    socket.close();
                } catch (IOException closeFailure) {
                    exception.addSuppressed(closeFailure);
                }
            }
            throw exception;
        }
    }

    public boolean isConnected() {
        return connected && !socket.isClosed();
    }

    public List<String> listFiles() throws IOException {
        return withOperation(() -> {
            long requestId = nextRequestId();
            send(new Frame(MessageType.LIST_REQUEST, requestId, new byte[0]));
            Frame response = receiveExpected(requestId, MessageType.LIST_RESPONSE);
            return decodeResponse(response, PayloadCodec::decodeListResponse).filenames();
        });
    }

    public TransferReceipt upload(Path source) throws IOException {
        Objects.requireNonNull(source, "source");
        Path filename = source.getFileName();
        if (filename == null) {
            throw new IOException("Upload source has no filename: " + source);
        }
        return upload(source, filename.toString());
    }

    public TransferReceipt upload(Path source, String remoteName) throws IOException {
        Objects.requireNonNull(source, "source");
        String filename = FilenameValidator.requireValid(remoteName);
        Path normalizedSource = source.toAbsolutePath().normalize();
        if (Files.isSymbolicLink(normalizedSource)
                || !Files.isRegularFile(normalizedSource, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Upload source is not a regular file: " + normalizedSource);
        }

        return withOperation(() -> {
            boolean accepted = false;
            try {
                long requestId = nextRequestId();
                send(new Frame(MessageType.KEEPALIVE, requestId, new byte[0]));
                long size = Files.size(normalizedSource);
                byte[] digest = hashUploadSource(normalizedSource, requestId);
                if (Files.size(normalizedSource) != size) {
                    throw new IOException("Upload source changed while its SHA-256 digest was calculated");
                }
                byte[] requestPayload = PayloadCodec.encodeUploadRequest(
                        new UploadRequest(filename, size, digest));
                send(new Frame(MessageType.UPLOAD_REQUEST, requestId, requestPayload));
                Frame response = receiveExpected(requestId, MessageType.UPLOAD_ACCEPTED);
                UploadAccepted uploadAccepted = decodeResponse(
                        response, PayloadCodec::decodeUploadAccepted);
                accepted = true;

                streamUpload(normalizedSource, size, digest, uploadAccepted);
                Frame completionFrame = receiveExpected(
                        uploadAccepted.transferId(), MessageType.TRANSFER_COMPLETE);
                TransferComplete completion = decodeResponse(
                        completionFrame, PayloadCodec::decodeTransferComplete);
                requireCompletion(completion, size, digest, uploadAccepted.transferId());
                return new TransferReceipt(filename, size, DigestUtil.toHex(digest));
            } catch (RemoteOperationException remoteError) {
                if (accepted) {
                    abortConnection(remoteError);
                }
                throw remoteError;
            } catch (IOException | RuntimeException failure) {
                if (accepted) {
                    abortConnection(failure);
                }
                throw failure;
            }
        });
    }

    public TransferReceipt download(String remoteName) throws IOException {
        String filename = FilenameValidator.requireValid(remoteName);
        return download(filename, Path.of(filename));
    }

    public TransferReceipt download(String remoteName, Path destination) throws IOException {
        String filename = FilenameValidator.requireValid(remoteName);
        Objects.requireNonNull(destination, "destination");
        byte[] requestPayload = PayloadCodec.encodeDownloadRequest(new DownloadRequest(filename));

        return withOperation(() -> {
            boolean requestSent = false;
            boolean transferStarted = false;
            try (AtomicDownload download = AtomicDownload.open(downloadRoot, destination)) {
                long requestId = nextRequestId();
                send(new Frame(MessageType.DOWNLOAD_REQUEST, requestId, requestPayload));
                requestSent = true;

                Frame metadataFrame = receiveExpected(requestId, MessageType.DOWNLOAD_METADATA);
                DownloadMetadata metadata = decodeResponse(
                        metadataFrame, PayloadCodec::decodeDownloadMetadata);
                transferStarted = true;
                if (metadata.size() > maximumDownloadSize) {
                    throw new IOException(
                            "Download exceeds the configured "
                                    + maximumDownloadSize + "-byte limit");
                }
                if (!filename.equals(metadata.filename())) {
                    throw protocolFailure(
                            metadata.transferId(), "Download metadata names an unexpected file");
                }

                receiveDownload(download, metadata);
                Frame completionFrame = receiveExpected(
                        metadata.transferId(), MessageType.TRANSFER_COMPLETE);
                TransferComplete completion = decodeResponse(
                        completionFrame, PayloadCodec::decodeTransferComplete);
                requireCompletion(
                        completion, metadata.size(), metadata.sha256(), metadata.transferId());
                download.commit(metadata.size(), metadata.sha256());
                return new TransferReceipt(
                        filename, metadata.size(), DigestUtil.toHex(metadata.sha256()));
            } catch (RemoteOperationException remoteError) {
                if (transferStarted) {
                    abortConnection(remoteError);
                }
                throw remoteError;
            } catch (IOException | RuntimeException failure) {
                if (requestSent) {
                    abortConnection(failure);
                }
                throw failure;
            }
        });
    }

    public void delete(String remoteName) throws IOException {
        String filename = FilenameValidator.requireValid(remoteName);
        byte[] payload = PayloadCodec.encodeDeleteRequest(new DeleteRequest(filename));
        withOperation(() -> {
            long requestId = nextRequestId();
            send(new Frame(MessageType.DELETE_REQUEST, requestId, payload));
            expectSuccess(requestId);
            return null;
        });
    }

    public void disconnect() throws IOException {
        lockOperation();
        try {
            disconnectWhileLocked();
        } finally {
            operationLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (!operationLock.tryLock()) {
            closeTransport();
            return;
        }
        try {
            disconnectWhileLocked();
        } finally {
            operationLock.unlock();
        }
    }

    private void handshake() throws IOException {
        long requestId = nextRequestId();
        send(new Frame(MessageType.HELLO, requestId, new byte[0]));
        expectSuccess(requestId);
    }

    private void disconnectWhileLocked() throws IOException {
        if (!isConnected()) {
            return;
        }
        IOException failure = null;
        try {
            long requestId = nextRequestId();
            send(new Frame(MessageType.DISCONNECT, requestId, new byte[0]));
            expectSuccess(requestId);
        } catch (IOException exception) {
            failure = exception;
        } finally {
            try {
                closeTransport();
            } catch (IOException closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void streamUpload(
            Path source, long declaredSize, byte[] declaredDigest, UploadAccepted accepted)
            throws IOException {
        int chunkSize = accepted.chunkSize();
        byte[] buffer = new byte[chunkSize];
        MessageDigest streamedDigest = DigestUtil.newSha256();
        long remaining = declaredSize;
        int sequence = 0;

        try (InputStream file = new BufferedInputStream(Files.newInputStream(source), chunkSize)) {
            if (remaining == 0) {
                if (file.read() != -1) {
                    throw new IOException("Upload source grew before it could be transferred");
                }
                requireStableUploadDigest(streamedDigest.digest(), declaredDigest);
                sendChunk(accepted.transferId(), sequence, true, new byte[0]);
                return;
            }

            while (remaining > 0) {
                int wanted = (int) Math.min(buffer.length, remaining);
                readFully(file, buffer, wanted);
                streamedDigest.update(buffer, 0, wanted);
                remaining -= wanted;
                boolean finalChunk = remaining == 0;

                if (finalChunk) {
                    if (file.read() != -1) {
                        throw new IOException("Upload source grew while it was being transferred");
                    }
                    requireStableUploadDigest(streamedDigest.digest(), declaredDigest);
                }

                if (!finalChunk && sequence == Integer.MAX_VALUE) {
                    throw new IOException("Upload contains too many chunks");
                }
                sendChunk(
                        accepted.transferId(),
                        sequence,
                        finalChunk,
                        Arrays.copyOf(buffer, wanted));
                if (!finalChunk) {
                    sequence++;
                }
            }
        }
    }

    private void sendChunk(long transferId, int sequence, boolean finalChunk, byte[] data)
            throws IOException {
        byte[] payload = PayloadCodec.encodeFileChunk(new FileChunk(sequence, finalChunk, data));
        send(new Frame(MessageType.FILE_CHUNK, transferId, payload));
    }

    private byte[] hashUploadSource(Path source, long requestId) throws IOException {
        MessageDigest digest = DigestUtil.newSha256();
        byte[] buffer = new byte[ProtocolConstants.MAX_CHUNK_BYTES];
        long nextKeepalive = System.nanoTime() + PREPARATION_KEEPALIVE_NANOS;
        try (InputStream file = new BufferedInputStream(
                Files.newInputStream(source), ProtocolConstants.MAX_CHUNK_BYTES)) {
            int read;
            while ((read = file.read(buffer)) != -1) {
                if (read > 0) {
                    digest.update(buffer, 0, read);
                }
                long now = System.nanoTime();
                if (now - nextKeepalive >= 0) {
                    ensureConnected();
                    send(new Frame(MessageType.KEEPALIVE, requestId, new byte[0]));
                    nextKeepalive = now + PREPARATION_KEEPALIVE_NANOS;
                }
            }
        }
        return digest.digest();
    }

    private void receiveDownload(AtomicDownload download, DownloadMetadata metadata)
            throws IOException {
        int expectedSequence = 0;
        boolean finalChunk = false;
        while (!finalChunk) {
            Frame chunkFrame = receiveExpected(metadata.transferId(), MessageType.FILE_CHUNK);
            FileChunk chunk = decodeResponse(chunkFrame, PayloadCodec::decodeFileChunk);
            if (chunk.sequence() != expectedSequence) {
                throw protocolFailure(
                        metadata.transferId(),
                        "Expected download chunk " + expectedSequence + " but received " + chunk.sequence());
            }
            byte[] data = chunk.data();
            if (data.length > metadata.chunkSize()) {
                throw protocolFailure(
                        metadata.transferId(), "Download chunk exceeds the negotiated size");
            }

            long projectedSize;
            try {
                projectedSize = Math.addExact(download.bytesWritten(), data.length);
            } catch (ArithmeticException overflow) {
                throw protocolFailure(metadata.transferId(), "Download byte count overflowed");
            }
            if (projectedSize > metadata.size()) {
                throw protocolFailure(metadata.transferId(), "Download exceeds its declared size");
            }
            if (chunk.finalChunk() != (projectedSize == metadata.size())) {
                throw protocolFailure(
                        metadata.transferId(), "Final-chunk flag does not match the declared file size");
            }

            boolean receivedFinalChunk = chunk.finalChunk();
            if (!receivedFinalChunk && expectedSequence == Integer.MAX_VALUE) {
                throw protocolFailure(metadata.transferId(), "Download contains too many chunks");
            }
            download.write(data);
            finalChunk = receivedFinalChunk;
            if (!finalChunk) {
                expectedSequence++;
            }
        }
    }

    private void requireStableUploadDigest(byte[] actual, byte[] expected) throws IOException {
        if (!DigestUtil.matches(expected, actual)) {
            throw new IOException("Upload source changed while it was being transferred");
        }
    }

    private void requireCompletion(
            TransferComplete completion, long expectedSize, byte[] expectedDigest, long transferId)
            throws IOException {
        if (completion.totalBytes() != expectedSize
                || !DigestUtil.matches(expectedDigest, completion.sha256())) {
            ProtocolException failure = protocolFailure(
                    transferId, "Transfer completion does not match the announced size and SHA-256");
            abortConnection(failure);
            throw failure;
        }
    }

    private void expectSuccess(long correlationId) throws IOException {
        Frame response = receiveExpected(correlationId, MessageType.SUCCESS);
        decodeResponse(response, PayloadCodec::decodeSuccess);
    }

    private Frame receiveExpected(long correlationId, MessageType expected) throws IOException {
        Frame frame;
        do {
            try {
                frame = FrameCodec.read(input);
            } catch (IOException exception) {
                abortConnection(exception);
                throw exception;
            }
            if (frame == null) {
                EOFException exception = new EOFException("Server closed the connection");
                abortConnection(exception);
                throw exception;
            }
            if (frame.correlationId() != correlationId) {
                ProtocolException exception = protocolFailure(
                        correlationId,
                        "Expected correlation ID " + correlationId
                                + " but received " + frame.correlationId());
                abortConnection(exception);
                throw exception;
            }
        } while (frame.type() == MessageType.KEEPALIVE);
        if (frame.type() == MessageType.ERROR) {
            PayloadCodec.Error error = decodeResponse(frame, PayloadCodec::decodeError);
            throw new RemoteOperationException(error.code(), correlationId, error.message());
        }
        if (frame.type() != expected) {
            ProtocolException exception = protocolFailure(
                    correlationId,
                    "Expected " + expected + " but received " + frame.type());
            abortConnection(exception);
            throw exception;
        }
        return frame;
    }

    private <T> T decodeResponse(Frame frame, Decoder<T> decoder) throws IOException {
        try {
            return decoder.decode(frame.payload());
        } catch (ProtocolException exception) {
            abortConnection(exception);
            throw exception;
        }
    }

    private void send(Frame frame) throws IOException {
        try {
            FrameCodec.write(output, frame);
            output.flush();
        } catch (IOException exception) {
            abortConnection(exception);
            throw exception;
        }
    }

    private long nextRequestId() throws IOException {
        long requestId = requestIds.getAndIncrement();
        if (requestId <= 0) {
            IOException failure = new IOException("Request ID space exhausted");
            abortConnection(failure);
            throw failure;
        }
        return requestId;
    }

    private <T> T withOperation(IoOperation<T> operation) throws IOException {
        lockOperation();
        try {
            ensureConnected();
            return operation.execute();
        } finally {
            operationLock.unlock();
        }
    }

    private void lockOperation() throws InterruptedIOException {
        try {
            operationLock.lockInterruptibly();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            InterruptedIOException failure = new InterruptedIOException(
                    "Interrupted while waiting for another client operation");
            failure.initCause(interrupted);
            throw failure;
        }
    }

    private void ensureConnected() throws IOException {
        if (!isConnected()) {
            throw new IOException("Client is not connected");
        }
    }

    private ProtocolException protocolFailure(long correlationId, String message) {
        return new ProtocolException(ErrorCode.MALFORMED_FRAME, correlationId, false, message);
    }

    private void abortConnection(Throwable primary) {
        connected = false;
        try {
            socket.close();
        } catch (IOException closeFailure) {
            if (primary != null) {
                primary.addSuppressed(closeFailure);
            }
        }
    }

    private void closeTransport() throws IOException {
        connected = false;
        socket.close();
    }

    private static void readFully(InputStream input, byte[] buffer, int length) throws IOException {
        int offset = 0;
        while (offset < length) {
            int read = input.read(buffer, offset, length - offset);
            if (read == -1) {
                throw new EOFException("Upload source became shorter while it was being transferred");
            }
            if (read == 0) {
                int single = input.read();
                if (single == -1) {
                    throw new EOFException("Upload source became shorter while it was being transferred");
                }
                buffer[offset++] = (byte) single;
            } else {
                offset += read;
            }
        }
    }

    @FunctionalInterface
    private interface IoOperation<T> {
        T execute() throws IOException;
    }

    @FunctionalInterface
    private interface Decoder<T> {
        T decode(byte[] payload) throws ProtocolException;
    }
}
