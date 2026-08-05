package filewire.server;

import filewire.protocol.ErrorCode;
import filewire.protocol.Frame;
import filewire.protocol.FrameCodec;
import filewire.protocol.MessageType;
import filewire.protocol.PayloadCodec;
import filewire.protocol.PayloadCodec.DeleteRequest;
import filewire.protocol.PayloadCodec.DownloadMetadata;
import filewire.protocol.PayloadCodec.DownloadRequest;
import filewire.protocol.PayloadCodec.FileChunk;
import filewire.protocol.PayloadCodec.ListResponse;
import filewire.protocol.PayloadCodec.Success;
import filewire.protocol.PayloadCodec.TransferComplete;
import filewire.protocol.PayloadCodec.UploadAccepted;
import filewire.protocol.PayloadCodec.UploadRequest;
import filewire.protocol.ProtocolConstants;
import filewire.protocol.ProtocolException;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** Isolated state machine for one TCP connection. */
final class ClientSession implements Runnable, AutoCloseable {
    private static final System.Logger LOGGER = System.getLogger(ClientSession.class.getName());

    private final long sessionId;
    private final Socket socket;
    private final TransferService transfers;
    private final Runnable closedCallback;
    private final BufferedInputStream input;
    private final BufferedOutputStream output;
    private final Object writeLock = new Object();
    private final AtomicBoolean closing = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean finished = new AtomicBoolean();

    private boolean helloReceived;
    private UploadState uploadState;
    private long responseCorrelationId;

    ClientSession(long sessionId, Socket socket, TransferService transfers, Runnable closedCallback)
            throws IOException {
        if (sessionId <= 0) {
            throw new IllegalArgumentException("sessionId must be positive");
        }
        this.sessionId = sessionId;
        this.socket = Objects.requireNonNull(socket, "socket");
        this.transfers = Objects.requireNonNull(transfers, "transfers");
        this.closedCallback = Objects.requireNonNull(closedCallback, "closedCallback");
        input = new BufferedInputStream(socket.getInputStream());
        output = new BufferedOutputStream(socket.getOutputStream());
    }

    long sessionId() {
        return sessionId;
    }

    @Override
    public void run() {
        LOGGER.log(System.Logger.Level.INFO, "Client session {0} connected", sessionId);
        try {
            while (!closing.get()) {
                Frame frame;
                try {
                    frame = FrameCodec.read(input);
                } catch (ProtocolException exception) {
                    logProtocolFailure(exception);
                    if (exception.replySafe()) {
                        sendError(exception.correlationId(), exception.errorCode(), exception.getMessage());
                    }
                    break;
                }
                if (frame == null) {
                    break;
                }
                responseCorrelationId = frame.correlationId();
                try {
                    process(frame);
                } catch (RequestException exception) {
                    LOGGER.log(
                            System.Logger.Level.WARNING,
                            "Request rejected in session {0} ({1}): {2}",
                            sessionId,
                            exception.errorCode(),
                            exception.getMessage());
                    boolean uploadFailed = frame.type() == MessageType.FILE_CHUNK;
                    if (uploadFailed) {
                        abortUpload();
                    }
                    sendError(responseCorrelationId, exception.errorCode(), exception.getMessage());
                    if (uploadFailed) {
                        break;
                    }
                } catch (ProtocolException exception) {
                    sendError(responseCorrelationId, exception.errorCode(), exception.getMessage());
                    break;
                } catch (IOException exception) {
                    abortUpload();
                    sendError(responseCorrelationId, ErrorCode.IO_FAILURE, "The server could not complete the I/O operation");
                    LOGGER.log(
                            System.Logger.Level.WARNING,
                            "I/O operation failed in session " + sessionId,
                            exception);
                    if (frame.type() == MessageType.FILE_CHUNK) {
                        break;
                    }
                } catch (RuntimeException exception) {
                    abortUpload();
                    sendError(responseCorrelationId, ErrorCode.INTERNAL_ERROR, "Internal server error");
                    LOGGER.log(
                            System.Logger.Level.ERROR,
                            "Unexpected request failure in session " + sessionId,
                            exception);
                    break;
                } finally {
                    responseCorrelationId = 0;
                }
            }
        } catch (SocketTimeoutException exception) {
            LOGGER.log(System.Logger.Level.INFO, "Client session {0} timed out", sessionId);
        } catch (SocketException exception) {
            if (!closing.get()) {
                LOGGER.log(System.Logger.Level.WARNING, "Socket failure in session " + sessionId, exception);
            }
        } catch (IOException exception) {
            if (!closing.get()) {
                LOGGER.log(System.Logger.Level.WARNING, "Connection failure in session " + sessionId, exception);
            }
        } finally {
            close();
            LOGGER.log(System.Logger.Level.INFO, "Client session {0} disconnected", sessionId);
        }
    }

    void rejectBusy() {
        try {
            sendError(1, ErrorCode.SERVER_BUSY, "Server connection limit reached");
        } catch (IOException ignored) {
            // The peer may have already disconnected while its rejection was being written.
        } finally {
            close();
        }
    }

    @Override
    public void close() {
        closing.set(true);
        if (closed.compareAndSet(false, true)) {
            abortUpload();
            try {
                transfers.abortSession(sessionId);
            } catch (IOException exception) {
                LOGGER.log(System.Logger.Level.WARNING, "Could not clean transfer state for session " + sessionId, exception);
            }
            try {
                socket.close();
            } catch (IOException exception) {
                LOGGER.log(System.Logger.Level.DEBUG, "Could not close session socket " + sessionId, exception);
            }
        }
        finishOnce();
    }

    private void process(Frame frame) throws IOException {
        if (!helloReceived && frame.type() != MessageType.HELLO && frame.type() != MessageType.DISCONNECT) {
            throw new RequestException(ErrorCode.INVALID_REQUEST, "HELLO must complete before other operations");
        }
        if (uploadState != null
                && frame.type() != MessageType.FILE_CHUNK
                && frame.type() != MessageType.KEEPALIVE
                && frame.type() != MessageType.DISCONNECT) {
            throw new RequestException(ErrorCode.TRANSFER_CONFLICT, "Finish or cancel the active upload first");
        }

        switch (frame.type()) {
            case HELLO -> handleHello(frame);
            case LIST_REQUEST -> handleList(frame);
            case UPLOAD_REQUEST -> handleUploadRequest(frame);
            case FILE_CHUNK -> handleUploadChunk(frame);
            case DOWNLOAD_REQUEST -> handleDownload(frame);
            case DELETE_REQUEST -> handleDelete(frame);
            case DISCONNECT -> handleDisconnect(frame);
            case KEEPALIVE -> {
                // One-way activity marker used while a peer performs a long digest pass.
            }
            case LIST_RESPONSE, UPLOAD_ACCEPTED, DOWNLOAD_METADATA, TRANSFER_COMPLETE, SUCCESS, ERROR ->
                    throw new RequestException(
                            ErrorCode.UNSUPPORTED_MESSAGE,
                            "Message type is not valid as a client request: " + frame.type());
        }
    }

    private void handleHello(Frame frame) throws IOException {
        if (helloReceived) {
            throw new RequestException(ErrorCode.INVALID_REQUEST, "HELLO was already received");
        }
        helloReceived = true;
        send(new Frame(
                MessageType.SUCCESS,
                frame.correlationId(),
                PayloadCodec.encodeSuccess(new Success("FileWire session established"))));
    }

    private void handleList(Frame frame) throws IOException {
        send(new Frame(
                MessageType.LIST_RESPONSE,
                frame.correlationId(),
                PayloadCodec.encodeListResponse(new ListResponse(transfers.listFiles()))));
    }

    private void handleUploadRequest(Frame frame) throws IOException {
        UploadRequest request = PayloadCodec.decodeUploadRequest(frame.payload());
        UploadTransaction upload = transfers.beginUpload(
                sessionId,
                request.filename(),
                request.size(),
                request.sha256());
        uploadState = new UploadState(upload.transferId(), request.size(), request.sha256());
        send(new Frame(
                MessageType.UPLOAD_ACCEPTED,
                frame.correlationId(),
                PayloadCodec.encodeUploadAccepted(new UploadAccepted(
                        upload.transferId(),
                        ProtocolConstants.MAX_CHUNK_BYTES))));
        LOGGER.log(
                System.Logger.Level.INFO,
                "Upload {0} started in session {1}: {2}",
                upload.transferId(),
                sessionId,
                request.filename());
    }

    private void handleUploadChunk(Frame frame) throws IOException {
        UploadState state = uploadState;
        if (state == null || frame.correlationId() != state.transferId()) {
            throw new RequestException(ErrorCode.TRANSFER_NOT_FOUND, "No matching upload is active");
        }
        FileChunk chunk = PayloadCodec.decodeFileChunk(frame.payload());
        boolean completed;
        try {
            completed = transfers.receiveUploadChunk(
                    sessionId,
                    frame.correlationId(),
                    chunk.sequence(),
                    chunk.finalChunk(),
                    chunk.data());
        } catch (IOException exception) {
            abortUpload();
            throw exception;
        }
        if (completed) {
            uploadState = null;
            send(new Frame(
                    MessageType.TRANSFER_COMPLETE,
                    frame.correlationId(),
                    PayloadCodec.encodeTransferComplete(new TransferComplete(state.size(), state.digest()))));
            LOGGER.log(
                    System.Logger.Level.INFO,
                    "Upload {0} completed in session {1}",
                    state.transferId(),
                    sessionId);
        }
    }

    private void handleDownload(Frame frame) throws IOException {
        DownloadRequest request = PayloadCodec.decodeDownloadRequest(frame.payload());
        try (DownloadTransfer transfer = transfers.beginDownload(
                sessionId,
                request.filename(),
                () -> send(new Frame(MessageType.KEEPALIVE, frame.correlationId(), new byte[0])))) {
            DownloadSource source = transfer.source();
            LOGGER.log(
                    System.Logger.Level.INFO,
                    "Download {0} started in session {1}: {2}",
                    transfer.transferId(),
                    sessionId,
                    source.filename());
            send(new Frame(
                    MessageType.DOWNLOAD_METADATA,
                    frame.correlationId(),
                    PayloadCodec.encodeDownloadMetadata(new DownloadMetadata(
                            transfer.transferId(),
                            source.filename(),
                            source.size(),
                            source.digest(),
                            ProtocolConstants.MAX_CHUNK_BYTES))));
            responseCorrelationId = transfer.transferId();
            streamDownload(transfer);
            send(new Frame(
                    MessageType.TRANSFER_COMPLETE,
                    transfer.transferId(),
                    PayloadCodec.encodeTransferComplete(new TransferComplete(source.size(), source.digest()))));
            LOGGER.log(
                    System.Logger.Level.INFO,
                    "Download {0} completed in session {1}: {2}",
                    transfer.transferId(),
                    sessionId,
                    source.filename());
        }
    }

    private void streamDownload(DownloadTransfer transfer) throws IOException {
        DownloadSource source = transfer.source();
        if (source.size() == 0) {
            sendChunk(transfer.transferId(), 0, true, new byte[0]);
            return;
        }

        byte[] buffer = new byte[ProtocolConstants.MAX_CHUNK_BYTES];
        long sent = 0;
        int sequence = 0;
        while (sent < source.size()) {
            int requested = (int) Math.min(buffer.length, source.size() - sent);
            byte[] readBuffer = requested == buffer.length ? buffer : new byte[requested];
            int read = source.read(readBuffer);
            if (read < 0) {
                throw new IOException("Stored file ended before its declared download size");
            }
            if (read == 0) {
                continue;
            }
            byte[] chunk = read == readBuffer.length ? readBuffer.clone() : Arrays.copyOf(readBuffer, read);
            sent += read;
            boolean finalChunk = sent == source.size();
            if (!finalChunk && sequence == Integer.MAX_VALUE) {
                throw new IOException("Stored file requires too many chunks");
            }
            sendChunk(transfer.transferId(), sequence, finalChunk, chunk);
            if (!finalChunk) {
                sequence++;
            }
        }
    }

    private void sendChunk(long transferId, int sequence, boolean finalChunk, byte[] data) throws IOException {
        send(new Frame(
                MessageType.FILE_CHUNK,
                transferId,
                PayloadCodec.encodeFileChunk(new FileChunk(sequence, finalChunk, data))));
    }

    private void handleDelete(Frame frame) throws IOException {
        DeleteRequest request = PayloadCodec.decodeDeleteRequest(frame.payload());
        transfers.delete(request.filename());
        send(new Frame(
                MessageType.SUCCESS,
                frame.correlationId(),
                PayloadCodec.encodeSuccess(new Success("Deleted " + request.filename()))));
    }

    private void handleDisconnect(Frame frame) throws IOException {
        send(new Frame(
                MessageType.SUCCESS,
                frame.correlationId(),
                PayloadCodec.encodeSuccess(new Success("Disconnected"))));
        closing.set(true);
    }

    private void sendError(long correlationId, ErrorCode code, String message) throws IOException {
        if (correlationId <= 0) {
            return;
        }
        send(new Frame(
                MessageType.ERROR,
                correlationId,
                PayloadCodec.encodeError(new PayloadCodec.Error(code, message))));
    }

    private void send(Frame frame) throws IOException {
        synchronized (writeLock) {
            if (socket.isClosed()) {
                throw new SocketException("Session socket is closed");
            }
            FrameCodec.write(output, frame);
            output.flush();
        }
    }

    private void abortUpload() {
        uploadState = null;
        try {
            transfers.abortSession(sessionId);
        } catch (IOException exception) {
            LOGGER.log(System.Logger.Level.WARNING, "Could not abort upload for session " + sessionId, exception);
        }
    }

    private void finishOnce() {
        if (finished.compareAndSet(false, true)) {
            closedCallback.run();
        }
    }

    private void logProtocolFailure(ProtocolException exception) {
        LOGGER.log(
                System.Logger.Level.WARNING,
                "Invalid protocol frame in session " + sessionId + ": " + exception.getMessage());
    }

    private record UploadState(long transferId, long size, byte[] digest) {
        private UploadState {
            digest = digest.clone();
        }

        @Override
        public byte[] digest() {
            return digest.clone();
        }
    }
}
