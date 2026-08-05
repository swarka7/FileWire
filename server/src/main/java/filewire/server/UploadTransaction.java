package filewire.server;

import filewire.protocol.DigestUtil;
import filewire.protocol.ErrorCode;
import filewire.protocol.ProtocolConstants;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Objects;

/** Stateful streaming upload. All terminal paths release resources exactly once. */
final class UploadTransaction implements AutoCloseable {
    private final long transferId;
    private final StorageService storage;
    private final Path temporaryFile;
    private final Path target;
    private final long expectedSize;
    private final byte[] expectedDigest;
    private final MessageDigest actualDigest = DigestUtil.newSha256();
    private final OutputStream output;
    private final Runnable terminalCallback;

    private int nextChunkNumber;
    private long receivedSize;
    private boolean terminal;
    private boolean committed;

    UploadTransaction(
            long transferId,
            StorageService storage,
            Path temporaryFile,
            Path target,
            long expectedSize,
            byte[] expectedDigest,
            Runnable terminalCallback) throws IOException {
        this.transferId = transferId;
        this.storage = Objects.requireNonNull(storage, "storage");
        this.temporaryFile = Objects.requireNonNull(temporaryFile, "temporaryFile");
        this.target = Objects.requireNonNull(target, "target");
        this.expectedSize = expectedSize;
        this.expectedDigest = expectedDigest.clone();
        this.terminalCallback = Objects.requireNonNull(terminalCallback, "terminalCallback");
        output = new BufferedOutputStream(Files.newOutputStream(
                temporaryFile,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING));
    }

    long transferId() {
        return transferId;
    }

    synchronized boolean receive(int chunkNumber, boolean finalChunk, byte[] bytes) throws IOException {
        Objects.requireNonNull(bytes, "bytes");
        ensureActive();
        if (chunkNumber != nextChunkNumber) {
            fail(new RequestException(ErrorCode.INVALID_REQUEST, "Unexpected upload chunk number"));
        }
        if (bytes.length > ProtocolConstants.MAX_CHUNK_BYTES) {
            fail(new RequestException(ErrorCode.INVALID_REQUEST, "Upload chunk exceeds the protocol limit"));
        }
        if (!finalChunk && bytes.length == 0) {
            fail(new RequestException(ErrorCode.INVALID_REQUEST, "Only the final upload chunk may be empty"));
        }

        long newSize;
        try {
            newSize = Math.addExact(receivedSize, bytes.length);
        } catch (ArithmeticException exception) {
            fail(new RequestException(ErrorCode.INVALID_REQUEST, "Upload size overflow", exception));
            return false; // unreachable
        }
        if (newSize > expectedSize) {
            fail(new RequestException(ErrorCode.INVALID_REQUEST, "Upload exceeds its declared size"));
        }
        if (finalChunk && newSize != expectedSize) {
            fail(new RequestException(ErrorCode.INVALID_REQUEST, "Final upload size does not match metadata"));
        }
        if (!finalChunk && newSize == expectedSize) {
            fail(new RequestException(ErrorCode.INVALID_REQUEST, "Upload reached its declared size without a final chunk"));
        }
        if (!finalChunk && nextChunkNumber == Integer.MAX_VALUE) {
            fail(new RequestException(ErrorCode.INVALID_REQUEST, "Upload contains too many chunks"));
        }

        try {
            output.write(bytes);
            actualDigest.update(bytes);
            receivedSize = newSize;
            if (finalChunk) {
                finish();
                return true;
            }
            nextChunkNumber++;
            return false;
        } catch (IOException exception) {
            abortAfterFailure(exception);
            throw exception;
        }
    }

    synchronized boolean committed() {
        return committed;
    }

    synchronized long receivedSize() {
        return receivedSize;
    }

    @Override
    public synchronized void close() throws IOException {
        if (!terminal) {
            abortAfterFailure(null);
        }
    }

    private void finish() throws IOException {
        IOException closeFailure = closeOutput();
        if (closeFailure != null) {
            abortAfterFailure(closeFailure);
            throw closeFailure;
        }
        byte[] receivedDigest = actualDigest.digest();
        if (!DigestUtil.matches(expectedDigest, receivedDigest)) {
            RequestException mismatch = new RequestException(
                    ErrorCode.INTEGRITY_MISMATCH,
                    "Upload SHA-256 digest does not match metadata");
            abortAfterFailure(mismatch);
            throw mismatch;
        }

        try {
            storage.commitTemporaryUpload(temporaryFile, target);
            committed = true;
            terminate();
        } catch (IOException exception) {
            abortAfterFailure(exception);
            throw exception;
        }
    }

    private void ensureActive() throws RequestException {
        if (terminal) {
            throw new RequestException(ErrorCode.TRANSFER_NOT_FOUND, "Upload is no longer active");
        }
    }

    private void fail(RequestException failure) throws IOException {
        abortAfterFailure(failure);
        throw failure;
    }

    private void abortAfterFailure(IOException original) throws IOException {
        if (terminal) {
            return;
        }
        IOException cleanupFailure = closeOutput();
        try {
            storage.deleteTemporaryFile(temporaryFile);
        } catch (IOException exception) {
            if (cleanupFailure == null) {
                cleanupFailure = exception;
            } else {
                cleanupFailure.addSuppressed(exception);
            }
        } finally {
            terminate();
        }
        if (cleanupFailure != null) {
            if (original != null) {
                original.addSuppressed(cleanupFailure);
            } else {
                throw cleanupFailure;
            }
        }
    }

    private IOException closeOutput() {
        try {
            output.close();
            return null;
        } catch (IOException exception) {
            return exception;
        }
    }

    private void terminate() {
        if (!terminal) {
            terminal = true;
            terminalCallback.run();
        }
    }
}
