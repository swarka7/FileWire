package filewire.server;

import filewire.protocol.ErrorCode;
import filewire.protocol.ProtocolConstants;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** Coordinates transfer IDs, per-session state, destination reservations, and cleanup. */
final class TransferService implements AutoCloseable {
    private final StorageService storage;
    private final AtomicIdGenerator transferIds = new AtomicIdGenerator();
    private final Map<Long, Long> activeTransferBySession = new ConcurrentHashMap<>();
    private final Map<Long, UploadTransaction> uploadsByTransfer = new ConcurrentHashMap<>();
    private final Map<Long, DownloadTransfer> downloadsByTransfer = new ConcurrentHashMap<>();
    private final Map<Path, Long> uploadReservations = new ConcurrentHashMap<>();
    private final Map<Path, Integer> activeDownloads = new ConcurrentHashMap<>();
    private final Object filenameActivityLock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean();

    TransferService(StorageService storage) {
        this.storage = storage;
    }

    UploadTransaction beginUpload(
            long sessionId,
            String filename,
            long expectedSize,
            byte[] expectedDigest) throws IOException {
        requireOpen();
        if (expectedSize < 0 || expectedSize > storage.maximumFileSize()) {
            throw new RequestException(ErrorCode.INVALID_REQUEST, "Upload size exceeds the configured limit");
        }
        if (expectedDigest == null || expectedDigest.length != ProtocolConstants.SHA256_BYTES) {
            throw new RequestException(ErrorCode.INVALID_REQUEST, "Upload metadata must contain a SHA-256 digest");
        }

        Path target = storage.resolveFile(filename);
        long transferId = transferIds.nextId();
        if (activeTransferBySession.putIfAbsent(sessionId, transferId) != null) {
            throw new RequestException(ErrorCode.TRANSFER_CONFLICT, "The session already has an active transfer");
        }

        Path temporaryFile = null;
        boolean reserved = false;
        try {
            synchronized (filenameActivityLock) {
                if (uploadReservations.putIfAbsent(target, transferId) != null) {
                    throw new RequestException(
                            ErrorCode.TRANSFER_CONFLICT,
                            "Another upload already reserved this filename");
                }
                reserved = true;
                storage.requireUploadTargetAvailable(target);
                temporaryFile = storage.createTemporaryUpload(transferId);
            }

            Path createdTemporaryFile = temporaryFile;
            UploadTransaction transaction = new UploadTransaction(
                    transferId,
                    storage,
                    createdTemporaryFile,
                    target,
                    expectedSize,
                    expectedDigest,
                    () -> releaseUpload(sessionId, transferId, target));
            uploadsByTransfer.put(transferId, transaction);
            if (closed.get()) {
                transaction.close();
                throw new IOException("Transfer service is closed");
            }
            return transaction;
        } catch (IOException | RuntimeException exception) {
            activeTransferBySession.remove(sessionId, transferId);
            if (reserved) {
                uploadReservations.remove(target, transferId);
            }
            if (temporaryFile != null) {
                try {
                    storage.deleteTemporaryFile(temporaryFile);
                } catch (IOException cleanupFailure) {
                    exception.addSuppressed(cleanupFailure);
                }
            }
            throw exception;
        }
    }

    boolean receiveUploadChunk(
            long sessionId,
            long transferId,
            int sequence,
            boolean finalChunk,
            byte[] data) throws IOException {
        Long sessionTransfer = activeTransferBySession.get(sessionId);
        UploadTransaction upload = uploadsByTransfer.get(transferId);
        if (sessionTransfer == null || sessionTransfer.longValue() != transferId || upload == null) {
            throw new RequestException(ErrorCode.TRANSFER_NOT_FOUND, "No matching upload is active");
        }
        return upload.receive(sequence, finalChunk, data);
    }

    DownloadTransfer beginDownload(long sessionId, String filename) throws IOException {
        return beginDownload(sessionId, filename, () -> {
        });
    }

    DownloadTransfer beginDownload(
            long sessionId,
            String filename,
            DownloadSource.Progress progress) throws IOException {
        requireOpen();
        Path target = storage.resolveFile(filename);
        long transferId = transferIds.nextId();
        if (activeTransferBySession.putIfAbsent(sessionId, transferId) != null) {
            throw new RequestException(ErrorCode.TRANSFER_CONFLICT, "The session already has an active transfer");
        }

        boolean downloadCounted = false;
        try {
            synchronized (filenameActivityLock) {
                activeDownloads.merge(target, 1, Integer::sum);
                downloadCounted = true;
            }
            DownloadSource source = storage.openDownload(filename, progress);
            DownloadTransfer transfer = new DownloadTransfer(
                    transferId,
                    source,
                    () -> releaseDownload(sessionId, transferId, target));
            downloadsByTransfer.put(transferId, transfer);
            if (closed.get()) {
                transfer.close();
                throw new IOException("Transfer service is closed");
            }
            return transfer;
        } catch (IOException | RuntimeException exception) {
            activeTransferBySession.remove(sessionId, transferId);
            if (downloadCounted) {
                decrementDownload(target);
            }
            throw exception;
        }
    }

    List<String> listFiles() throws IOException {
        requireOpen();
        return storage.listFiles();
    }

    void delete(String filename) throws IOException {
        requireOpen();
        Path target = storage.resolveFile(filename);
        synchronized (filenameActivityLock) {
            if (uploadReservations.containsKey(target) || activeDownloads.containsKey(target)) {
                throw new RequestException(ErrorCode.TRANSFER_CONFLICT, "The file is part of an active transfer");
            }
            storage.delete(filename);
        }
    }

    void abortSession(long sessionId) throws IOException {
        Long transferId = activeTransferBySession.get(sessionId);
        if (transferId == null) {
            return;
        }

        IOException failure = null;
        UploadTransaction upload = uploadsByTransfer.get(transferId);
        if (upload != null) {
            try {
                upload.close();
            } catch (IOException exception) {
                failure = exception;
            }
        }
        DownloadTransfer download = downloadsByTransfer.get(transferId);
        if (download != null) {
            try {
                download.close();
            } catch (IOException exception) {
                if (failure == null) {
                    failure = exception;
                } else {
                    failure.addSuppressed(exception);
                }
            }
        }
        activeTransferBySession.remove(sessionId, transferId);
        if (failure != null) {
            throw failure;
        }
    }

    int activeTransferCount() {
        return activeTransferBySession.size();
    }

    int reservationCount() {
        return uploadReservations.size();
    }

    long nextTransferIdForTest() {
        return transferIds.nextId();
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        IOException failure = null;
        for (Long sessionId : new ArrayList<>(activeTransferBySession.keySet())) {
            try {
                abortSession(sessionId);
            } catch (IOException exception) {
                if (failure == null) {
                    failure = exception;
                } else {
                    failure.addSuppressed(exception);
                }
            }
        }
        try {
            storage.close();
        } catch (IOException exception) {
            if (failure == null) {
                failure = exception;
            } else {
                failure.addSuppressed(exception);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void requireOpen() throws IOException {
        if (closed.get()) {
            throw new IOException("Transfer service is closed");
        }
    }

    private void releaseUpload(long sessionId, long transferId, Path target) {
        uploadsByTransfer.remove(transferId);
        activeTransferBySession.remove(sessionId, transferId);
        uploadReservations.remove(target, transferId);
    }

    private void releaseDownload(long sessionId, long transferId, Path target) {
        downloadsByTransfer.remove(transferId);
        activeTransferBySession.remove(sessionId, transferId);
        decrementDownload(target);
    }

    private void decrementDownload(Path target) {
        synchronized (filenameActivityLock) {
            activeDownloads.computeIfPresent(target, (ignored, count) -> count == 1 ? null : count - 1);
        }
    }
}
