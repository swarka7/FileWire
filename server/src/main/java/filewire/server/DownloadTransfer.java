package filewire.server;

import java.io.IOException;
import java.util.Objects;

/** A tracked download handle that releases its session slot when closed. */
final class DownloadTransfer implements AutoCloseable {
    private final long transferId;
    private final DownloadSource source;
    private final Runnable terminalCallback;
    private boolean closed;

    DownloadTransfer(long transferId, DownloadSource source, Runnable terminalCallback) {
        this.transferId = transferId;
        this.source = Objects.requireNonNull(source, "source");
        this.terminalCallback = Objects.requireNonNull(terminalCallback, "terminalCallback");
    }

    long transferId() {
        return transferId;
    }

    DownloadSource source() {
        return source;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            source.close();
        } finally {
            terminalCallback.run();
        }
    }
}
