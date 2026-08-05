package filewire.server;

import filewire.protocol.DigestUtil;
import filewire.protocol.ErrorCode;
import filewire.protocol.ProtocolConstants;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Objects;

/** An open, bounded-memory download whose digest was computed from the same file handle. */
final class DownloadSource implements AutoCloseable {
    private static final long PROGRESS_INTERVAL_NANOS = Duration.ofMillis(
            ProtocolConstants.PREPARATION_KEEPALIVE_INTERVAL_MILLIS).toNanos();
    private final String filename;
    private final FileChannel channel;
    private final long size;
    private final byte[] digest;

    private DownloadSource(String filename, FileChannel channel, long size, byte[] digest) {
        this.filename = filename;
        this.channel = channel;
        this.size = size;
        this.digest = digest;
    }

    static DownloadSource open(String filename, Path path, long maximumFileSize) throws IOException {
        return open(filename, path, maximumFileSize, () -> {
        });
    }

    static DownloadSource open(
            String filename,
            Path path,
            long maximumFileSize,
            Progress progress) throws IOException {
        Objects.requireNonNull(progress, "progress");
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
        try {
            long size = channel.size();
            if (size < 0 || size > maximumFileSize) {
                throw new RequestException(ErrorCode.INVALID_REQUEST, "File exceeds the configured transfer limit");
            }

            MessageDigest digest = DigestUtil.newSha256();
            ByteBuffer buffer = ByteBuffer.allocate(ProtocolConstants.MAX_CHUNK_BYTES);
            long digested = 0;
            long nextProgress = System.nanoTime() + PROGRESS_INTERVAL_NANOS;
            int read;
            while ((read = channel.read(buffer)) != -1) {
                if (read > 0) {
                    digested += read;
                    buffer.flip();
                    digest.update(buffer);
                    buffer.clear();
                }
                long now = System.nanoTime();
                if (now - nextProgress >= 0) {
                    progress.report();
                    nextProgress = now + PROGRESS_INTERVAL_NANOS;
                }
            }
            if (digested != size || channel.size() != size) {
                throw new RequestException(ErrorCode.TRANSFER_CONFLICT, "File changed while preparing download");
            }
            channel.position(0);
            return new DownloadSource(filename, channel, size, digest.digest());
        } catch (IOException | RuntimeException exception) {
            try {
                channel.close();
            } catch (IOException closeFailure) {
                exception.addSuppressed(closeFailure);
            }
            throw exception;
        }
    }

    String filename() {
        return filename;
    }

    long size() {
        return size;
    }

    byte[] digest() {
        return digest.clone();
    }

    int read(byte[] buffer) throws IOException {
        return channel.read(ByteBuffer.wrap(buffer));
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    @FunctionalInterface
    interface Progress {
        void report() throws IOException;
    }
}
